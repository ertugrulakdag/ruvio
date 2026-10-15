//! Geospatial index stored as sorted-set scores.
//!
//! A member's score is the 52-bit WGS84 geohash used by Redis, Garnet, and
//! Dragonfly, so `TYPE` stays `zset` and the member is stored once. Search
//! walks the nine geohash boxes that cover the shape instead of the whole set.

const LAT_MIN: f64 = -85.05112878;
const LAT_MAX: f64 = 85.05112878;
const LON_MIN: f64 = -180.0;
const LON_MAX: f64 = 180.0;
const STEP_MAX: u8 = 26;
const EARTH_RADIUS_M: f64 = 6_372_797.560856;
const MERCATOR_MAX: f64 = 20_037_726.37;
const DEG_TO_RAD: f64 = 0.017453292519943295769236907684886;
const GEO_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

#[derive(Clone, Copy)]
pub enum Shape {
    Circle { radius_m: f64 },
    Box { width_m: f64, height_m: f64 },
}

#[derive(Clone, Copy)]
pub struct BoxHash {
    bits: u64,
    step: u8,
}

pub struct Place {
    pub longitude: f64,
    pub latitude: f64,
    pub distance_m: f64,
}

pub fn encode_score(longitude: f64, latitude: f64) -> Option<f64> {
    let hash = encode(longitude, latitude, STEP_MAX, LAT_MIN, LAT_MAX)?;

    Some(align52(hash) as f64)
}

pub fn decode_score(score: f64) -> Option<(f64, f64)> {
    if !score.is_finite() || score < 0.0 {
        return None;
    }

    let bits = score as u64;

    if bits as f64 != score {
        return None;
    }

    let area = decode_area(HashBits {
        bits,
        step: STEP_MAX,
    })?;
    let mut longitude = (area.lon_min + area.lon_max) / 2.0;
    let mut latitude = (area.lat_min + area.lat_max) / 2.0;

    longitude = longitude.clamp(LON_MIN, LON_MAX);
    latitude = latitude.clamp(LAT_MIN, LAT_MAX);

    Some((longitude, latitude))
}

pub fn standard_geohash(longitude: f64, latitude: f64) -> Option<[u8; 11]> {
    let hash = encode(longitude, latitude, STEP_MAX, -90.0, 90.0)?;
    let mut text = [b'0'; 11];

    for (index, byte) in text.iter_mut().enumerate().take(10) {
        let shift = 52 - (index + 1) * 5;
        let alphabet = ((hash.bits >> shift) & 0x1f) as usize;

        *byte = GEO_ALPHABET[alphabet];
    }

    Some(text)
}

pub fn distance_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lon1 = lon1 * DEG_TO_RAD;
    let lon2 = lon2 * DEG_TO_RAD;
    let v = ((lon2 - lon1) / 2.0).sin();

    if v == 0.0 {
        return EARTH_RADIUS_M * (lat2 * DEG_TO_RAD - lat1 * DEG_TO_RAD).abs();
    }

    let lat1 = lat1 * DEG_TO_RAD;
    let lat2 = lat2 * DEG_TO_RAD;
    let u = ((lat2 - lat1) / 2.0).sin();
    let a = u * u + lat1.cos() * lat2.cos() * v * v;

    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

pub fn search_boxes(longitude: f64, latitude: f64, shape: Shape) -> Vec<BoxHash> {
    let bounds = bounding_box(longitude, latitude, shape);
    let radius_m = match shape {
        Shape::Circle { radius_m } => radius_m,
        Shape::Box { width_m, height_m } => (width_m / 2.0).hypot(height_m / 2.0),
    };
    let mut steps = estimate_steps(radius_m, latitude);
    let Some(mut center) = encode(longitude, latitude, steps, LAT_MIN, LAT_MAX) else {
        return Vec::new();
    };
    let mut neighbors = neighbor_hashes(center);

    if steps > 1 && !covers(&neighbors, &bounds) {
        steps -= 1;

        if let Some(smaller) = encode(longitude, latitude, steps, LAT_MIN, LAT_MAX) {
            center = smaller;
            neighbors = neighbor_hashes(center);
        }
    }

    if steps >= 2
        && let Some(area) = decode_area(center)
    {
        if area.lat_min < bounds.min_lat {
            neighbors.south = HashBits::default();
            neighbors.south_west = HashBits::default();
            neighbors.south_east = HashBits::default();
        }

        if area.lat_max > bounds.max_lat {
            neighbors.north = HashBits::default();
            neighbors.north_east = HashBits::default();
            neighbors.north_west = HashBits::default();
        }

        if area.lon_min < bounds.min_lon {
            neighbors.west = HashBits::default();
            neighbors.north_west = HashBits::default();
            neighbors.south_west = HashBits::default();
        }

        if area.lon_max > bounds.max_lon {
            neighbors.east = HashBits::default();
            neighbors.north_east = HashBits::default();
            neighbors.south_east = HashBits::default();
        }
    }

    let cells = [
        center,
        neighbors.north,
        neighbors.south,
        neighbors.east,
        neighbors.west,
        neighbors.north_east,
        neighbors.north_west,
        neighbors.south_east,
        neighbors.south_west,
    ];
    let mut boxes = Vec::with_capacity(cells.len());
    let mut previous: Option<HashBits> = None;

    for cell in cells {
        if cell.bits == 0 && cell.step == 0 {
            continue;
        }

        if previous.is_some_and(|seen| seen.bits == cell.bits && seen.step == cell.step) {
            continue;
        }

        boxes.push(BoxHash {
            bits: cell.bits,
            step: cell.step,
        });
        previous = Some(cell);
    }

    boxes
}

pub fn score_window(hash: BoxHash) -> (f64, f64) {
    let min = align52(HashBits {
        bits: hash.bits,
        step: hash.step,
    });
    let max = align52(HashBits {
        bits: hash.bits.wrapping_add(1),
        step: hash.step,
    });

    (min as f64, max as f64)
}

pub fn place_if_inside(
    shape: Shape,
    center_lon: f64,
    center_lat: f64,
    score: f64,
) -> Option<Place> {
    let (longitude, latitude) = decode_score(score)?;
    let meters = distance_m(center_lon, center_lat, longitude, latitude);
    let inside = match shape {
        Shape::Circle { radius_m } => meters <= radius_m,
        Shape::Box { width_m, height_m } => {
            let lat_distance =
                EARTH_RADIUS_M * (latitude * DEG_TO_RAD - center_lat * DEG_TO_RAD).abs();

            if lat_distance > height_m / 2.0 {
                false
            } else {
                let lon_distance = distance_m(longitude, latitude, center_lon, latitude);

                lon_distance <= width_m / 2.0
            }
        }
    };

    if !inside {
        return None;
    }

    Some(Place {
        longitude,
        latitude,
        distance_m: meters,
    })
}

#[derive(Clone, Copy, Default)]
struct HashBits {
    bits: u64,
    step: u8,
}

struct Area {
    lon_min: f64,
    lon_max: f64,
    lat_min: f64,
    lat_max: f64,
}

struct Bounds {
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
}

struct Neighbors {
    north: HashBits,
    south: HashBits,
    east: HashBits,
    west: HashBits,
    north_east: HashBits,
    north_west: HashBits,
    south_east: HashBits,
    south_west: HashBits,
}

fn encode(longitude: f64, latitude: f64, step: u8, lat_min: f64, lat_max: f64) -> Option<HashBits> {
    if step == 0
        || step > 32
        || !(LON_MIN..=LON_MAX).contains(&longitude)
        || !(LAT_MIN..=LAT_MAX).contains(&latitude)
        || latitude < lat_min
        || latitude > lat_max
    {
        return None;
    }

    let scale = (1u64 << step) as f64;
    let lat = ((latitude - lat_min) / (lat_max - lat_min) * scale) as u32;
    let lon = ((longitude - LON_MIN) / (LON_MAX - LON_MIN) * scale) as u32;

    Some(HashBits {
        bits: interleave(lat, lon),
        step,
    })
}

fn decode_area(hash: HashBits) -> Option<Area> {
    if hash.step == 0 {
        return None;
    }

    let separated = deinterleave(hash.bits);
    let lat_bits = separated as u32;
    let lon_bits = (separated >> 32) as u32;
    let scale = (1u64 << hash.step) as f64;
    let lat_span = LAT_MAX - LAT_MIN;
    let lon_span = LON_MAX - LON_MIN;

    Some(Area {
        lat_min: LAT_MIN + (lat_bits as f64 / scale) * lat_span,
        lat_max: LAT_MIN + ((lat_bits as f64 + 1.0) / scale) * lat_span,
        lon_min: LON_MIN + (lon_bits as f64 / scale) * lon_span,
        lon_max: LON_MIN + ((lon_bits as f64 + 1.0) / scale) * lon_span,
    })
}

fn align52(hash: HashBits) -> u64 {
    let shift = 52 - u32::from(hash.step) * 2;

    hash.bits << shift
}

fn interleave(xlo: u32, ylo: u32) -> u64 {
    let mut x = u64::from(xlo);
    let mut y = u64::from(ylo);
    let masks = [
        0x0000_ffff_0000_ffff,
        0x00ff_00ff_00ff_00ff,
        0x0f0f_0f0f_0f0f_0f0f,
        0x3333_3333_3333_3333,
        0x5555_5555_5555_5555,
    ];
    let shifts = [16, 8, 4, 2, 1];

    for (mask, shift) in masks.into_iter().zip(shifts) {
        x = (x | (x << shift)) & mask;
        y = (y | (y << shift)) & mask;
    }

    x | (y << 1)
}

fn deinterleave(interleaved: u64) -> u64 {
    let mut x = interleaved;
    let mut y = interleaved >> 1;
    let masks = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0f0f_0f0f_0f0f_0f0f,
        0x00ff_00ff_00ff_00ff,
        0x0000_ffff_0000_ffff,
        0x0000_0000_ffff_ffff,
    ];
    let shifts = [0, 1, 2, 4, 8, 16];

    for (mask, shift) in masks.into_iter().zip(shifts) {
        x = (x | (x >> shift)) & mask;
        y = (y | (y >> shift)) & mask;
    }

    x | (y << 32)
}

fn move_x(hash: &mut HashBits, delta: i8) {
    if delta == 0 {
        return;
    }

    let width = 64 - hash.step * 2;
    let mut x = hash.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let y = hash.bits & 0x5555_5555_5555_5555;
    let zz = 0x5555_5555_5555_5555u64 >> width;

    if delta > 0 {
        x = x.wrapping_add(zz.wrapping_add(1));
    } else {
        x |= zz;
        x = x.wrapping_sub(zz.wrapping_add(1));
    }

    x &= 0xaaaa_aaaa_aaaa_aaaa >> width;
    hash.bits = x | y;
}

fn move_y(hash: &mut HashBits, delta: i8) {
    if delta == 0 {
        return;
    }

    let width = 64 - hash.step * 2;
    let x = hash.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let mut y = hash.bits & 0x5555_5555_5555_5555;
    let zz: u64 = 0xaaaa_aaaa_aaaa_aaaa >> width;

    if delta > 0 {
        y = y.wrapping_add(zz.wrapping_add(1));
    } else {
        y |= zz;
        y = y.wrapping_sub(zz.wrapping_add(1));
    }

    y &= 0x5555_5555_5555_5555 >> width;
    hash.bits = x | y;
}

fn neighbor_hashes(center: HashBits) -> Neighbors {
    let mut neighbors = Neighbors {
        north: center,
        south: center,
        east: center,
        west: center,
        north_east: center,
        north_west: center,
        south_east: center,
        south_west: center,
    };

    move_y(&mut neighbors.north, 1);
    move_y(&mut neighbors.south, -1);
    move_x(&mut neighbors.east, 1);
    move_x(&mut neighbors.west, -1);
    move_x(&mut neighbors.north_east, 1);
    move_y(&mut neighbors.north_east, 1);
    move_x(&mut neighbors.north_west, -1);
    move_y(&mut neighbors.north_west, 1);
    move_x(&mut neighbors.south_east, 1);
    move_y(&mut neighbors.south_east, -1);
    move_x(&mut neighbors.south_west, -1);
    move_y(&mut neighbors.south_west, -1);
    neighbors
}

fn covers(neighbors: &Neighbors, bounds: &Bounds) -> bool {
    let Some(north) = decode_area(neighbors.north) else {
        return false;
    };
    let Some(south) = decode_area(neighbors.south) else {
        return false;
    };
    let Some(east) = decode_area(neighbors.east) else {
        return false;
    };
    let Some(west) = decode_area(neighbors.west) else {
        return false;
    };
    north.lat_max >= bounds.max_lat
        && south.lat_min <= bounds.min_lat
        && east.lon_max >= bounds.max_lon
        && west.lon_min <= bounds.min_lon
}

fn bounding_box(longitude: f64, latitude: f64, shape: Shape) -> Bounds {
    let (half_width, half_height) = match shape {
        Shape::Circle { radius_m } => (radius_m, radius_m),
        Shape::Box { width_m, height_m } => (width_m / 2.0, height_m / 2.0),
    };
    let lat_delta = (half_height / EARTH_RADIUS_M).to_degrees();
    let long_delta_top =
        (half_width / EARTH_RADIUS_M / (latitude + lat_delta).to_radians().cos()).to_degrees();
    let long_delta_bottom =
        (half_width / EARTH_RADIUS_M / (latitude - lat_delta).to_radians().cos()).to_degrees();
    let (west, east) = if latitude < 0.0 {
        (longitude - long_delta_bottom, longitude + long_delta_bottom)
    } else {
        (longitude - long_delta_top, longitude + long_delta_top)
    };

    Bounds {
        min_lon: west,
        min_lat: latitude - lat_delta,
        max_lon: east,
        max_lat: latitude + lat_delta,
    }
}

fn estimate_steps(mut range_meters: f64, latitude: f64) -> u8 {
    if range_meters == 0.0 {
        return STEP_MAX;
    }

    let mut step = 1i32;

    while range_meters < MERCATOR_MAX {
        range_meters *= 2.0;
        step += 1;
    }

    step -= 2;

    if !(-66.0..=66.0).contains(&latitude) {
        step -= 1;

        if !(-80.0..=80.0).contains(&latitude) {
            step -= 1;
        }
    }

    step.clamp(1, i32::from(STEP_MAX)) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sicily_matches_the_redis_geohash_and_distance() {
        let palermo = standard_geohash(13.361389, 38.115556).unwrap();
        let catania = standard_geohash(15.087269, 37.502669).unwrap();

        assert_eq!(&palermo, b"sqc8b49rny0");
        assert_eq!(&catania, b"sqdtr74hyu0");

        let (palermo_lon, palermo_lat) =
            decode_score(encode_score(13.361389, 38.115556).unwrap()).unwrap();
        let (catania_lon, catania_lat) =
            decode_score(encode_score(15.087269, 37.502669).unwrap()).unwrap();
        let meters = distance_m(palermo_lon, palermo_lat, catania_lon, catania_lat);

        assert!((meters - 166_274.151_6).abs() < 0.001, "{meters}");
    }

    #[test]
    fn a_score_decodes_inside_its_cell() {
        let score = encode_score(13.361389, 38.115556).unwrap();
        let (longitude, latitude) = decode_score(score).unwrap();

        assert!((longitude - 13.361389).abs() < 0.00001);
        assert!((latitude - 38.115556).abs() < 0.00001);
    }

    #[test]
    fn a_radius_search_keeps_the_near_city() {
        let shape = Shape::Circle {
            radius_m: 100_000.0,
        };
        let boxes = search_boxes(13.361389, 38.115556, shape);
        let palermo = encode_score(13.361389, 38.115556).unwrap();
        let catania = encode_score(15.087269, 37.502669).unwrap();
        let covered = |score: f64| {
            boxes.iter().any(|hash| {
                let (min, max) = score_window(*hash);

                score >= min && score < max
            })
        };

        assert!(covered(palermo));
        assert!(place_if_inside(shape, 13.361389, 38.115556, palermo).is_some());
        assert!(place_if_inside(shape, 13.361389, 38.115556, catania).is_none());
    }
}
