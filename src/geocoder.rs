use anyhow::{Context, Result};
use arrow_array::{Array, Float64Array, ListArray, StringArray, StructArray};
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
use kiddo::{KdTree, SquaredEuclidean};
use memmap2::Mmap;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rkyv::ser::Serializer;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::fs::File;

/// Populated place record
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(compare(PartialEq))]
pub struct LoadingPlace {
    pub name: String,
    pub latitude: f64,
    pub longitude: f64,
}

/// The actual data stored in the geocoder file
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
pub struct ReverseGeocoderData {
    /// 3D KD-tree on unit sphere coordinates
    tree: KdTree<f64, 3>,

    /// Place storage indexed by item id in KD-tree
    places: Vec<LoadingPlace>,
}

enum GeocoderStorage {
    Owned(ReverseGeocoderData),
    Mapped(Mmap),
}

/// Reverse geocoder backed by a KD-tree
pub struct ReverseGeocoder {
    storage: GeocoderStorage,
}

impl ReverseGeocoder {
    /// Create a new empty reverse geocoder
    pub fn new() -> Self {
        ReverseGeocoder {
            storage: GeocoderStorage::Owned(ReverseGeocoderData {
                tree: KdTree::new(),
                places: Vec::new(),
            }),
        }
    }

    pub fn load_from_file(&mut self, path: &str) -> Result<()> {
        let bytes = std::fs::read(path)?;
        let archived = unsafe { rkyv::archived_root::<ReverseGeocoderData>(&bytes) };

        let data: ReverseGeocoderData =
            archived.deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new())?;
        println!("Loaded {} places", data.places.len());
        self.storage = GeocoderStorage::Owned(data);
        Ok(())
    }

    pub fn zero_copy_from_file(&mut self, path: &str) -> Result<()> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        // Verify it can be archived
        let _ = unsafe { rkyv::archived_root::<ReverseGeocoderData>(&mmap) };

        self.storage = GeocoderStorage::Mapped(mmap);
        Ok(())
    }

    pub fn load_from_overture(&mut self, path: &str) -> Result<()> {
        let file = File::open(path)
            .with_context(|| format!("Failed to open GeoParquet file: {}", path))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("Failed to create Parquet reader for: {}", path))?;

        let geoparquet_metadata = builder
            .geoparquet_metadata()
            .ok_or_else(|| anyhow::anyhow!("No GeoParquet metadata found"))??;

        let geoarrow_schema = builder
            .geoarrow_schema(&geoparquet_metadata, true, Default::default())
            .with_context(|| format!("Failed to create GeoArrow schema for: {}", path))?;

        let parquet_reader = builder
            .with_batch_size(65536)
            .build()
            .with_context(|| format!("Failed to build Parquet reader for: {}", path))?;
        let geoparquet_reader =
            GeoParquetRecordBatchReader::try_new(parquet_reader, geoarrow_schema)
                .with_context(|| format!("Failed to create GeoParquet reader for: {}", path))?;

        let mut tree = KdTree::new();
        let mut places: Vec<LoadingPlace> = Vec::new();

        let mut batch_index = 1;
        for batch_result in geoparquet_reader {
            let batch = batch_result
                .with_context(|| format!("Failed to read batch from GeoParquet file: {}", path))?;

            println!("Processing batch {}", batch_index);
            batch_index += 1;

            self.process_batch(batch, &mut tree, &mut places)?;
        }

        self.storage = GeocoderStorage::Owned(ReverseGeocoderData { tree, places });
        if let GeocoderStorage::Owned(data) = &self.storage {
            println!("Loaded {} places", data.places.len());
        }
        Ok(())
    }

    pub fn save_to_file(&self, path: &str) -> Result<()> {
        match &self.storage {
            GeocoderStorage::Owned(data) => {
                let mut serializer = AllocSerializer::<0>::default();
                serializer.serialize_value(data)?;
                let bytes = serializer.into_serializer().into_inner();
                std::fs::write(path, &bytes)?;
            }
            GeocoderStorage::Mapped(mmap) => {
                std::fs::write(path, mmap)?;
            }
        }
        Ok(())
    }

    /// Find nearest populated place to given coordinates.
    pub fn nearest_place(&self, latitude: f64, longitude: f64) -> Option<LoadingPlace> {
        let query = lat_lon_to_unit_sphere(latitude, longitude);

        match &self.storage {
            GeocoderStorage::Owned(data) => {
                let nearest = data.tree.nearest_one::<SquaredEuclidean>(&query);
                data.places.get(nearest.item as usize).cloned()
            }
            GeocoderStorage::Mapped(mmap) => {
                let archived = unsafe { rkyv::archived_root::<ReverseGeocoderData>(mmap) };
                let nearest = archived.tree.nearest_one::<SquaredEuclidean>(&query);
                let archived_place = archived.places.get(nearest.item as usize)?;

                archived_place
                    .deserialize(&mut rkyv::de::deserializers::SharedDeserializeMap::new())
                    .ok()
            }
        }
    }

    fn process_batch(
        &self,
        batch: arrow_array::RecordBatch,
        tree: &mut KdTree<f64, 3>,
        places: &mut Vec<LoadingPlace>,
    ) -> Result<()> {
        // Get the columns
        let subtype_column = batch.column_by_name("subtype");
        let names_column = batch.column_by_name("names");
        let names_struct: Option<&StructArray> =
            names_column.and_then(|col| col.as_any().downcast_ref::<StructArray>());
        let geometry_column = batch.column_by_name("geometry");
        let hierarchies_column = batch.column_by_name("hierarchies");
        let hierarchies_list: Option<&ListArray> =
            hierarchies_column.and_then(|col| col.as_any().downcast_ref::<ListArray>());

        if let (Some(names_struct), Some(geom_col)) = (names_struct, geometry_column) {
            let primary_column = names_struct.column_by_name("primary");
            let primary: Option<&StringArray> =
                primary_column.and_then(|col| col.as_any().downcast_ref::<StringArray>());
            let num_rows = batch.num_rows();

            for row in 0..num_rows {
                if !names_struct.is_valid(row) {
                    println!("Skipping invalid row {}", row);
                    continue;
                }

                if self.should_skip_subtype(subtype_column, row)? {
                    continue;
                }

                let primary_name = self.extract_primary_name(primary, row);
                let name = self.build_name_from_hierarchy(hierarchies_list, row, primary_name);

                if name.is_empty() {
                    println!("Skipping place with empty name");
                    continue;
                }

                if let Some((latitude, longitude)) = self.extract_coordinates(geom_col, row)
                    && let Some(place) =
                        self.create_or_update_place(name, latitude, longitude, tree, places)
                {
                    places.push(place);
                }
            }
        }
        Ok(())
    }

    fn should_skip_subtype(
        &self,
        subtype_column: Option<&std::sync::Arc<dyn arrow_array::Array>>,
        row: usize,
    ) -> Result<bool> {
        let skip_subtypes = [
            "microhood",
            "region",
            "macroregion",
            "dependency",
            "country",
        ];
        if let Some(subtype_arr) =
            subtype_column.and_then(|col| col.as_any().downcast_ref::<StringArray>())
            && subtype_arr.is_valid(row)
            && skip_subtypes.contains(&subtype_arr.value(row))
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn extract_primary_name(&self, primary: Option<&StringArray>, row: usize) -> Option<String> {
        if let Some(primary) = primary {
            if primary.is_valid(row) {
                Some(primary.value(row).to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn build_name_from_hierarchy(
        &self,
        hierarchies_list: Option<&ListArray>,
        row: usize,
        primary_name: Option<String>,
    ) -> String {
        if let Some(hierarchies_list) = hierarchies_list
            && hierarchies_list.is_valid(row)
        {
            // Get the list of hierarchies for this row
            let offsets = hierarchies_list.offsets();
            let start_offset = offsets[row] as usize;
            let end_offset = offsets[row + 1] as usize;

            let values = hierarchies_list.values();

            if let Some(values_list) = values.as_any().downcast_ref::<ListArray>() {
                if start_offset < end_offset {
                    let hierarchy_offset = values_list.offsets();
                    let h_start = hierarchy_offset[start_offset] as usize;
                    let h_end = hierarchy_offset[start_offset + 1] as usize;

                    let hierarchy_values = values_list.values();

                    if let Some(struct_array) =
                        hierarchy_values.as_any().downcast_ref::<StructArray>()
                    {
                        self.process_hierarchy(struct_array, h_start, h_end, primary_name)
                    } else {
                        println!("first_hierarchy is not a ListArray");
                        primary_name.unwrap_or_default()
                    }
                } else {
                    println!("hierarchies_for_row is empty");
                    primary_name.unwrap_or_default()
                }
            } else {
                println!("hierarchies_list is empty");
                primary_name.unwrap_or_default()
            }
        } else {
            println!("hierarchies_array is empty");
            primary_name.unwrap_or_default()
        }
    }

    fn process_hierarchy(
        &self,
        struct_array: &StructArray,
        h_start: usize,
        h_end: usize,
        primary_name: Option<String>,
    ) -> String {
        let local_subtypes = ["microhood", "neighborhood", "macrohood", "borough"];
        let mut result_parts: Vec<String> = Vec::new();
        let mut found_local = false;

        for i in (h_start..h_end).rev() {
            let subtype_col = struct_array.column_by_name("subtype");
            let name_col = struct_array.column_by_name("name");

            if let (Some(subtype_arr), Some(name_arr)) = (subtype_col, name_col)
                && let Some(subtype_str) = subtype_arr.as_any().downcast_ref::<StringArray>()
                && let Some(name_str) = name_arr.as_any().downcast_ref::<StringArray>()
                && subtype_str.is_valid(i)
                && name_str.is_valid(i)
            {
                let subtype = subtype_str.value(i);
                let item_name = name_str.value(i);

                if local_subtypes.contains(&subtype) {
                    if !found_local {
                        result_parts.push(item_name.to_string());
                        found_local = true;
                    }
                } else {
                    result_parts.push(item_name.to_string());
                    break;
                }
            }
        }

        if !result_parts.is_empty() {
            result_parts.join(", ")
        } else {
            println!("result_parts is empty");
            primary_name.unwrap_or_default()
        }
    }

    fn extract_coordinates(
        &self,
        geom_col: &std::sync::Arc<dyn arrow_array::Array>,
        row: usize,
    ) -> Option<(f64, f64)> {
        if let Some(struct_array) = geom_col.as_any().downcast_ref::<StructArray>()
            && struct_array.is_valid(row)
        {
            let x_column = struct_array.column_by_name("x");
            let y_column = struct_array.column_by_name("y");

            if let (Some(x_arr), Some(y_arr)) = (x_column, y_column)
                && let (Some(x_float), Some(y_float)) = (
                    x_arr.as_any().downcast_ref::<Float64Array>(),
                    y_arr.as_any().downcast_ref::<Float64Array>(),
                )
                && x_float.is_valid(row)
                && y_float.is_valid(row)
            {
                let longitude = x_float.value(row);
                let latitude = y_float.value(row);
                return Some((latitude, longitude));
            }
        }
        None
    }

    fn create_or_update_place(
        &self,
        name: String,
        latitude: f64,
        longitude: f64,
        tree: &mut KdTree<f64, 3>,
        places: &mut [LoadingPlace],
    ) -> Option<LoadingPlace> {
        let place = LoadingPlace {
            name,
            latitude,
            longitude,
        };
        let idx = places.len() as u64;
        let point_sphere = lat_lon_to_unit_sphere(latitude, longitude);

        if !places.is_empty() {
            let existing = tree.nearest_one::<SquaredEuclidean>(&point_sphere);
            let existing_idx = existing.item;
            let existing_place = places.get(existing_idx as usize).unwrap().clone();
            let distance = haversine_meters(
                latitude,
                longitude,
                existing_place.latitude,
                existing_place.longitude,
            );

            if distance < 100.0 {
                let existing_point =
                    lat_lon_to_unit_sphere(existing_place.latitude, existing_place.longitude);
                tree.remove(&existing_point, existing.item);
                tree.add(&point_sphere, existing_idx);
                *places.get_mut(existing_idx as usize).unwrap() = place;
                return None;
            }
        }

        tree.add(&point_sphere, idx);
        Some(place)
    }
}

/// Convert lat/lon to 3D unit sphere coordinates.
///
/// This avoids issues with:
/// - dateline wrapping
/// - polar distortion
/// - naïve Euclidean distance on lat/lon
fn lat_lon_to_unit_sphere(lat: f64, lon: f64) -> [f64; 3] {
    let lat_rad = lat.to_radians();
    let lon_rad = lon.to_radians();

    let x = lat_rad.cos() * lon_rad.cos();
    let y = lat_rad.cos() * lon_rad.sin();
    let z = lat_rad.sin();

    [x, y, z]
}

fn haversine_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;

    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();

    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();

    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);

    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_M * c
}
