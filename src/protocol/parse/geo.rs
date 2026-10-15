use super::*;

pub(in crate::protocol) fn parse_geo_add(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.is_empty() {
        return Err(ProtocolError::Invalid(
            "GEOADD expects a key and at least one longitude, latitude, and member",
        ));
    }

    let key = arguments.remove(0);
    let mut options = SortedSetAddOptions::default();
    let mut index = 0;

    while index < arguments.len() {
        let token = arguments[index].as_ref();

        if token.eq_ignore_ascii_case(b"nx") {
            if options.condition == SortedSetCondition::Exists {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.condition = SortedSetCondition::NotExists;
        } else if token.eq_ignore_ascii_case(b"xx") {
            if options.condition == SortedSetCondition::NotExists {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            options.condition = SortedSetCondition::Exists;
        } else if token.eq_ignore_ascii_case(b"ch") {
            options.changed = true;
        } else {
            break;
        }

        index += 1;
    }

    let rest = arguments.split_off(index);

    if rest.is_empty() || !rest.len().is_multiple_of(3) {
        return Err(ProtocolError::Invalid(
            "GEOADD expects longitude, latitude, and member triples",
        ));
    }

    let mut members = Vec::with_capacity(rest.len() / 3);
    let mut rest = rest.into_iter();

    while let Some(longitude) = rest.next() {
        let latitude = rest.next().expect("triple length checked");
        let member = rest.next().expect("triple length checked");

        members.push((parse_float(&longitude)?, parse_float(&latitude)?, member));
    }

    Ok(Command::GeoAdd {
        key,
        members,
        options,
    })
}

pub(in crate::protocol) fn parse_geo_dist(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() != 3 && arguments.len() != 4 {
        return Err(ProtocolError::Invalid(
            "GEODIST expects a key, two members, and an optional unit",
        ));
    }

    let unit = if arguments.len() == 4 {
        parse_geo_unit(&arguments.pop().expect("length checked"))?
    } else {
        1.0
    };

    let to = arguments.pop().expect("length checked");
    let from = arguments.pop().expect("length checked");
    let key = arguments.pop().expect("length checked");

    Ok(Command::GeoDist {
        key,
        from,
        to,
        unit,
    })
}

pub(in crate::protocol) fn parse_geo_pos(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid(
            "GEOPOS expects a key and at least one member",
        ));
    }

    let key = arguments.remove(0);

    Ok(Command::GeoPos {
        key,
        members: arguments,
    })
}

pub(in crate::protocol) fn parse_geo_hash(
    mut arguments: Vec<Bytes<'_>>,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < 2 {
        return Err(ProtocolError::Invalid(
            "GEOHASH expects a key and at least one member",
        ));
    }

    let key = arguments.remove(0);

    Ok(Command::GeoHash {
        key,
        members: arguments,
    })
}

pub(in crate::protocol) fn parse_geo_radius(
    mut arguments: Vec<Bytes<'_>>,
    by_member: bool,
) -> Result<Command<'_>, ProtocolError> {
    let minimum = if by_member { 4 } else { 5 };

    if arguments.len() < minimum {
        return Err(ProtocolError::Invalid(
            "GEORADIUS expects a key, center, radius, and unit",
        ));
    }

    let key = arguments.remove(0);
    let center = if by_member {
        GeoCenter::Member(arguments.remove(0))
    } else {
        let longitude = parse_float(&arguments.remove(0))?;
        let latitude = parse_float(&arguments.remove(0))?;

        GeoCenter::Coordinates(longitude, latitude)
    };

    let radius = parse_float(&arguments.remove(0))?;
    let unit = parse_geo_unit(&arguments.remove(0))?;

    if radius < 0.0 {
        return Err(ProtocolError::Invalid("radius must be non-negative"));
    }

    let options = parse_geo_options(&arguments, true)?;

    Ok(Command::GeoSearch {
        key,
        center,
        area: GeoArea::Radius(radius * unit),
        unit,
        descending: options.descending,
        ordered: options.ordered,
        count: options.count,
        any: options.any,
        with_dist: options.with_dist,
        with_hash: options.with_hash,
        with_coord: options.with_coord,
        store: options.store,
        query: if by_member {
            GeoCommand::RadiusByMember
        } else {
            GeoCommand::Radius
        },
    })
}

pub(in crate::protocol) fn parse_geo_search(
    mut arguments: Vec<Bytes<'_>>,
    store: bool,
) -> Result<Command<'_>, ProtocolError> {
    if arguments.len() < if store { 2 } else { 1 } {
        return Err(ProtocolError::Invalid(
            "GEOSEARCH expects a key, a center, and a shape",
        ));
    }

    let destination = if store {
        Some(arguments.remove(0))
    } else {
        None
    };

    let key = arguments.remove(0);
    let mut center = None;
    let mut area = None;
    let mut unit = 1.0;
    let mut descending = false;
    let mut saw_order = false;
    let mut count = None;
    let mut any = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut with_coord = false;
    let mut storedist = false;
    let mut index = 0;

    while index < arguments.len() {
        let token = arguments[index].as_ref();

        if token.eq_ignore_ascii_case(b"frommember") {
            if center.is_some() || index + 1 >= arguments.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            center = Some(GeoCenter::Member(arguments[index + 1].clone()));
            index += 1;
        } else if token.eq_ignore_ascii_case(b"fromlonlat") {
            if center.is_some() || index + 2 >= arguments.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let longitude = parse_float(&arguments[index + 1])?;
            let latitude = parse_float(&arguments[index + 2])?;

            center = Some(GeoCenter::Coordinates(longitude, latitude));
            index += 2;
        } else if token.eq_ignore_ascii_case(b"byradius") {
            if area.is_some() || index + 2 >= arguments.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let radius = parse_float(&arguments[index + 1])?;

            unit = parse_geo_unit(&arguments[index + 2])?;

            if radius < 0.0 {
                return Err(ProtocolError::Invalid("radius must be non-negative"));
            }

            area = Some(GeoArea::Radius(radius * unit));
            index += 2;
        } else if token.eq_ignore_ascii_case(b"bybox") {
            if area.is_some() || index + 3 >= arguments.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let width = parse_float(&arguments[index + 1])?;
            let height = parse_float(&arguments[index + 2])?;

            unit = parse_geo_unit(&arguments[index + 3])?;

            if width < 0.0 || height < 0.0 {
                return Err(ProtocolError::Invalid(
                    "box dimensions must be non-negative",
                ));
            }

            area = Some(GeoArea::Box {
                width_m: width * unit,
                height_m: height * unit,
            });

            index += 3;
        } else if token.eq_ignore_ascii_case(b"withdist") {
            with_dist = true;
        } else if token.eq_ignore_ascii_case(b"withhash") {
            with_hash = true;
        } else if token.eq_ignore_ascii_case(b"withcoord") {
            with_coord = true;
        } else if token.eq_ignore_ascii_case(b"asc") {
            descending = false;
            saw_order = true;
        } else if token.eq_ignore_ascii_case(b"desc") {
            descending = true;
            saw_order = true;
        } else if token.eq_ignore_ascii_case(b"any") {
            any = true;
        } else if token.eq_ignore_ascii_case(b"count") {
            if index + 1 >= arguments.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let value = parse_usize(&arguments[index + 1])?;

            if value == 0 {
                return Err(ProtocolError::Invalid("COUNT must be > 0"));
            }

            count = Some(value);
            index += 1;
        } else if token.eq_ignore_ascii_case(b"storedist") && store {
            storedist = true;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 1;
    }

    let Some(center) = center else {
        return Err(ProtocolError::Invalid(
            "exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH",
        ));
    };

    let Some(area) = area else {
        return Err(ProtocolError::Invalid(
            "exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH",
        ));
    };

    if any && count.is_none() {
        return Err(ProtocolError::Invalid(
            "the ANY argument requires COUNT argument",
        ));
    }

    if store && (with_dist || with_hash || with_coord) {
        return Err(ProtocolError::Invalid(
            "GEOSEARCHSTORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
        ));
    }

    Ok(Command::GeoSearch {
        key,
        center,
        area,
        unit,
        descending,
        ordered: saw_order || (count.is_some() && !any),
        count,
        any,
        with_dist,
        with_hash,
        with_coord,
        store: destination.map(|destination| GeoStore {
            destination,
            distance: storedist,
        }),
        query: if store {
            GeoCommand::SearchStore
        } else {
            GeoCommand::Search
        },
    })
}

pub(in crate::protocol) struct GeoOptions<'a> {
    pub(in crate::protocol) descending: bool,
    pub(in crate::protocol) ordered: bool,
    pub(in crate::protocol) count: Option<usize>,
    pub(in crate::protocol) any: bool,
    pub(in crate::protocol) with_dist: bool,
    pub(in crate::protocol) with_hash: bool,
    pub(in crate::protocol) with_coord: bool,
    pub(in crate::protocol) store: Option<GeoStore<'a>>,
}

pub(in crate::protocol) fn parse_geo_options<'a>(
    arguments: &[Bytes<'a>],
    allow_store: bool,
) -> Result<GeoOptions<'a>, ProtocolError> {
    let mut descending = false;
    let mut saw_order = false;
    let mut count = None;
    let mut any = false;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut with_coord = false;
    let mut store = None;
    let mut index = 0;

    while index < arguments.len() {
        let token = arguments[index].as_ref();

        if token.eq_ignore_ascii_case(b"withdist") {
            with_dist = true;
        } else if token.eq_ignore_ascii_case(b"withhash") {
            with_hash = true;
        } else if token.eq_ignore_ascii_case(b"withcoord") {
            with_coord = true;
        } else if token.eq_ignore_ascii_case(b"asc") {
            descending = false;
            saw_order = true;
        } else if token.eq_ignore_ascii_case(b"desc") {
            descending = true;
            saw_order = true;
        } else if token.eq_ignore_ascii_case(b"any") {
            any = true;
        } else if token.eq_ignore_ascii_case(b"count") {
            if index + 1 >= arguments.len() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            let value = parse_usize(&arguments[index + 1])?;

            if value == 0 {
                return Err(ProtocolError::Invalid("COUNT must be > 0"));
            }

            count = Some(value);
            index += 1;
        } else if token.eq_ignore_ascii_case(b"storedist") && allow_store {
            if index + 1 >= arguments.len() || store.is_some() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            store = Some(GeoStore {
                destination: arguments[index + 1].clone(),
                distance: true,
            });

            index += 1;
        } else if token.eq_ignore_ascii_case(b"store") && allow_store {
            if index + 1 >= arguments.len() || store.is_some() {
                return Err(ProtocolError::Invalid("syntax error"));
            }

            store = Some(GeoStore {
                destination: arguments[index + 1].clone(),
                distance: false,
            });

            index += 1;
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }

        index += 1;
    }

    if any && count.is_none() {
        return Err(ProtocolError::Invalid(
            "the ANY argument requires COUNT argument",
        ));
    }

    if store.is_some() && (with_dist || with_hash || with_coord) {
        return Err(ProtocolError::Invalid(
            "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
        ));
    }

    let ordered = saw_order || (count.is_some() && !any);

    Ok(GeoOptions {
        descending,
        ordered,
        count,
        any,
        with_dist,
        with_hash,
        with_coord,
        store,
    })
}

pub(in crate::protocol) fn parse_geo_unit(bytes: &[u8]) -> Result<f64, ProtocolError> {
    if bytes.eq_ignore_ascii_case(b"m") {
        return Ok(1.0);
    }

    if bytes.eq_ignore_ascii_case(b"km") {
        return Ok(1_000.0);
    }

    if bytes.eq_ignore_ascii_case(b"ft") {
        return Ok(0.3048);
    }

    if bytes.eq_ignore_ascii_case(b"mi") {
        return Ok(1_609.34);
    }

    Err(ProtocolError::Invalid(
        "unsupported unit provided. please use M, KM, FT, MI",
    ))
}
