use criterion::black_box as bb;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage, Value};
use prost_types::field_descriptor_proto::Type as ProstFieldType;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use serde_json::Value as JsonValue;
use zeroparser::parser::ParsedMessage;
use zeroparser::types::FieldValueRef;
use zeroparser::MessageRegistry;

pub const BENCH_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/bench_descriptor_set.bin"));

pub const SAMPLE_DATA_JSON: &str = include_str!("../bench_sample_data.json");

pub mod proto {
    pub mod air_quality {
        include!(concat!(
            env!("OUT_DIR"),
            "/zeroparser.benches.air_quality.rs"
        ));
    }
    pub mod click_bench {
        include!(concat!(
            env!("OUT_DIR"),
            "/zeroparser.benches.click_bench.rs"
        ));
    }
    pub mod supported_nullable_types {
        include!(concat!(
            env!("OUT_DIR"),
            "/zeroparser.benches.supported_nullable_types.rs"
        ));
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ProstTypedKind {
    AirQuality,
    SupportedNullableTypes,
    ClickBench,
}

impl ProstTypedKind {
    fn from_name(name: &str) -> Self {
        match name {
            "AirQuality" => Self::AirQuality,
            "SupportedNullableTypes" => Self::SupportedNullableTypes,
            "ClickBench" => Self::ClickBench,
            other => panic!("no prost typed walker for message {other}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FieldKind {
    Scalar,
    RepeatedScalar,
    RepeatedMessage,
    Map,
}

const LABEL_REPEATED: i32 = 3;

fn classify_field(field: &FieldDescriptorProto, nested: &[DescriptorProto]) -> FieldKind {
    if field.label != Some(LABEL_REPEATED) {
        return FieldKind::Scalar;
    }
    if field.r#type() != ProstFieldType::Message {
        return FieldKind::RepeatedScalar;
    }
    let last = field
        .type_name
        .as_deref()
        .and_then(|n| n.rsplit('.').next())
        .unwrap_or("");
    let is_map = nested
        .iter()
        .find(|n| n.name.as_deref() == Some(last))
        .and_then(|n| n.options.as_ref().and_then(|o| o.map_entry))
        == Some(true);
    if is_map {
        FieldKind::Map
    } else {
        FieldKind::RepeatedMessage
    }
}

/// Pre-classified field numbers; lets the Zeroparser walk dispatch on kind
/// once per scenario instead of per field.
pub struct FieldSet {
    pub scalars: Vec<i32>,
    pub repeated_scalars: Vec<i32>,
    pub repeated_messages: Vec<i32>,
    pub maps: Vec<i32>,
}

impl FieldSet {
    fn from_descriptor(desc: &DescriptorProto) -> Self {
        let mut scalars = Vec::new();
        let mut repeated_scalars = Vec::new();
        let mut repeated_messages = Vec::new();
        let mut maps = Vec::new();
        for f in &desc.field {
            let Some(num) = f.number else { continue };
            match classify_field(f, &desc.nested_type) {
                FieldKind::Scalar => scalars.push(num),
                FieldKind::RepeatedScalar => repeated_scalars.push(num),
                FieldKind::RepeatedMessage => repeated_messages.push(num),
                FieldKind::Map => maps.push(num),
            }
        }
        Self {
            scalars,
            repeated_scalars,
            repeated_messages,
            maps,
        }
    }
}

pub struct BenchmarkConfig {
    pub registry: MessageRegistry,
    pub msg_desc: MessageDescriptor,
    pub fields: FieldSet,
    pub prost_typed: ProstTypedKind,
}

impl BenchmarkConfig {
    pub fn for_message(message_name: &str) -> Self {
        let file_desc_set =
            FileDescriptorSet::decode(BENCH_DESCRIPTOR_SET).expect("decode bench descriptor set");
        let (descriptor_proto, file_proto, package) =
            find_message_and_file(&file_desc_set, message_name);

        let mut registry_descriptor = descriptor_proto.clone();
        registry_descriptor.name = Some(format!("{package}.{message_name}"));
        let registry = MessageRegistry::from_descriptor(&registry_descriptor);

        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file_proto.clone())
            .expect("add file descriptor proto");
        let fq_name = format!("{package}.{message_name}");
        let msg_desc = pool
            .get_message_by_name(&fq_name)
            .expect("message descriptor not found in pool");

        let fields = FieldSet::from_descriptor(descriptor_proto);

        Self {
            registry,
            msg_desc,
            fields,
            prost_typed: ProstTypedKind::from_name(message_name),
        }
    }
}

fn find_message_and_file<'a>(
    file_desc_set: &'a FileDescriptorSet,
    message_name: &str,
) -> (
    &'a DescriptorProto,
    &'a prost_types::FileDescriptorProto,
    &'a str,
) {
    for file in &file_desc_set.file {
        for msg_desc in &file.message_type {
            if msg_desc.name.as_deref() == Some(message_name) {
                let package = file.package.as_deref().unwrap_or("");
                return (msg_desc, file, package);
            }
        }
    }
    panic!("message {message_name} not found in descriptor set");
}

fn json_to_proto_bytes(msg_desc: &MessageDescriptor, json: &str) -> Vec<u8> {
    let mut deserializer = serde_json::Deserializer::from_str(json);
    let msg = DynamicMessage::deserialize(msg_desc.clone(), &mut deserializer)
        .expect("deserialize JSON into proto message");
    deserializer
        .end()
        .expect("unexpected trailing content in JSON input");
    msg.encode_to_vec()
}

pub fn load_bench_sample(key: &str) -> String {
    let value: JsonValue = serde_json::from_str(SAMPLE_DATA_JSON).expect("parse sample data JSON");
    serde_json::to_string(&value[key]).expect("re-serialize sample data section")
}

pub fn bench_prost_reflect_decode(
    msg_desc: &MessageDescriptor,
    encoded_messages: &[Vec<u8>],
) -> u64 {
    let mut total_field_count = 0u64;
    for encoded_bytes in encoded_messages {
        let msg = DynamicMessage::decode(msg_desc.clone(), encoded_bytes.as_slice())
            .expect("decode dynamic message");
        for field in msg_desc.fields() {
            let value = msg.get_field(&field);
            bb(match value.as_ref() {
                Value::I32(v) => *v as u64,
                Value::I64(v) => *v as u64,
                Value::String(v) => v.len() as u64,
                Value::U32(v) => *v as u64,
                Value::U64(v) => *v,
                Value::Bool(v) => *v as u64,
                Value::F32(v) => v.to_bits() as u64,
                Value::F64(v) => v.to_bits(),
                Value::Bytes(v) => v.len() as u64,
                Value::EnumNumber(v) => *v as u64,
                Value::List(v) => v.len() as u64,
                Value::Map(v) => v.len() as u64,
                Value::Message(v) => v.descriptor().name().len() as u64,
            });
            total_field_count += 1;
        }
    }
    total_field_count
}

pub fn bench_prost_typed_decode(kind: ProstTypedKind, encoded_messages: &[Vec<u8>]) -> u64 {
    match kind {
        ProstTypedKind::AirQuality => walk_air_quality(encoded_messages),
        ProstTypedKind::SupportedNullableTypes => walk_supported_nullable_types(encoded_messages),
        ProstTypedKind::ClickBench => walk_click_bench(encoded_messages),
    }
}

fn walk_air_quality(encoded_messages: &[Vec<u8>]) -> u64 {
    use proto::air_quality::AirQuality;
    let mut total = 0u64;
    for bytes in encoded_messages {
        let m = AirQuality::decode(bytes.as_slice()).expect("decode AirQuality");
        bb(m.device_name.len() as u64);
        bb(m.temp as u64);
        bb(m.humidity as u64);
        total += 3;
    }
    total
}

fn walk_supported_nullable_types(encoded_messages: &[Vec<u8>]) -> u64 {
    use proto::supported_nullable_types::SupportedNullableTypes;
    let mut total = 0u64;
    for bytes in encoded_messages {
        let m = SupportedNullableTypes::decode(bytes.as_slice())
            .expect("decode SupportedNullableTypes");
        bb(m.approved as u64);
        bb(m.day_num as u64);
        bb(m.cost as u64);
        bb(m.discount.to_bits() as u64);
        bb(m.cost_with_discount.to_bits());
        bb(m.description.len() as u64);
        bb(m.photo.len() as u64);
        bb(m.tags.len() as u64);
        bb(m.activity_ratings.len() as u64);
        bb(m.day_activities.len() as u64);
        bb(m.contact_info.len() as u64);
        bb(m.byte_num as u64);
        bb(m.short_num as u64);
        total += 13;
    }
    total
}

fn walk_click_bench(encoded_messages: &[Vec<u8>]) -> u64 {
    use proto::click_bench::ClickBench;
    let mut total = 0u64;
    for bytes in encoded_messages {
        let m = ClickBench::decode(bytes.as_slice()).expect("decode ClickBench");
        bb(m.watch_id as u64);
        bb(m.java_enable as u64);
        bb(m.title.len() as u64);
        bb(m.good_event as u64);
        bb(m.event_time as u64);
        bb(m.event_date as u64);
        bb(m.counter_id as u64);
        bb(m.client_ip as u64);
        bb(m.region_id as u64);
        bb(m.user_id as u64);
        bb(m.counter_class as u64);
        bb(m.os as u64);
        bb(m.user_agent as u64);
        bb(m.url.len() as u64);
        bb(m.referer.len() as u64);
        bb(m.is_refresh as u64);
        bb(m.referer_category_id as u64);
        bb(m.referer_region_id as u64);
        bb(m.url_category_id as u64);
        bb(m.url_region_id as u64);
        bb(m.resolution_width as u64);
        bb(m.resolution_height as u64);
        bb(m.resolution_depth as u64);
        bb(m.flash_major as u64);
        bb(m.flash_minor as u64);
        bb(m.flash_minor2.len() as u64);
        bb(m.net_major as u64);
        bb(m.net_minor as u64);
        bb(m.user_agent_major as u64);
        bb(m.user_agent_minor.len() as u64);
        bb(m.cookie_enable as u64);
        bb(m.javascript_enable as u64);
        bb(m.is_mobile as u64);
        bb(m.mobile_phone as u64);
        bb(m.mobile_phone_model.len() as u64);
        bb(m.params.len() as u64);
        bb(m.ip_network_id as u64);
        bb(m.trafic_source_id as u64);
        bb(m.search_engine_id as u64);
        bb(m.search_phrase.len() as u64);
        bb(m.adv_engine_id as u64);
        bb(m.is_artifical as u64);
        bb(m.window_client_width as u64);
        bb(m.window_client_height as u64);
        bb(m.client_time_zone as u64);
        bb(m.client_event_time as u64);
        bb(m.silverlight_version1 as u64);
        bb(m.silverlight_version2 as u64);
        bb(m.silverlight_version3 as u64);
        bb(m.silverlight_version4 as u64);
        bb(m.page_charset.len() as u64);
        bb(m.code_version as u64);
        bb(m.is_link as u64);
        bb(m.is_download as u64);
        bb(m.is_not_bounce as u64);
        bb(m.f_uniq_id as u64);
        bb(m.original_url.len() as u64);
        bb(m.hid as u64);
        bb(m.is_old_counter as u64);
        bb(m.is_event as u64);
        bb(m.is_parameter as u64);
        bb(m.dont_count_hits as u64);
        bb(m.with_hash as u64);
        bb(m.hit_color.len() as u64);
        bb(m.local_event_time as u64);
        bb(m.age as u64);
        bb(m.sex as u64);
        bb(m.income as u64);
        bb(m.interests as u64);
        bb(m.robotness as u64);
        bb(m.remote_ip as u64);
        bb(m.window_name as u64);
        bb(m.opener_name as u64);
        bb(m.history_length as u64);
        bb(m.browser_language.len() as u64);
        bb(m.browser_country.len() as u64);
        bb(m.social_network.len() as u64);
        bb(m.social_action.len() as u64);
        bb(m.http_error as u64);
        bb(m.send_timing as u64);
        bb(m.dns_timing as u64);
        bb(m.connect_timing as u64);
        bb(m.response_start_timing as u64);
        bb(m.response_end_timing as u64);
        bb(m.fetch_timing as u64);
        bb(m.social_source_network_id as u64);
        bb(m.social_source_page.len() as u64);
        bb(m.param_price as u64);
        bb(m.param_order_id.len() as u64);
        bb(m.param_currency.len() as u64);
        bb(m.param_currency_id as u64);
        bb(m.openstat_service_name.len() as u64);
        bb(m.openstat_campaign_id.len() as u64);
        bb(m.openstat_ad_id.len() as u64);
        bb(m.openstat_source_id.len() as u64);
        bb(m.utm_source.len() as u64);
        bb(m.utm_medium.len() as u64);
        bb(m.utm_campaign.len() as u64);
        bb(m.utm_content.len() as u64);
        bb(m.utm_term.len() as u64);
        bb(m.from_tag.len() as u64);
        bb(m.has_gclid as u64);
        bb(m.referer_hash as u64);
        bb(m.url_hash as u64);
        bb(m.clid as u64);
        total += 105;
    }
    total
}

pub fn bench_zeroparser_decode(
    registry: &MessageRegistry,
    fields: &FieldSet,
    encoded_messages: &[Vec<u8>],
) -> u64 {
    let mut total_field_count = 0u64;
    for encoded_bytes in encoded_messages {
        let collected = ParsedMessage::parse(encoded_bytes, registry).expect("parse message");
        for &field_num in &fields.scalars {
            total_field_count += 1;
            match collected.get_scalar(field_num) {
                Some(scalar) => bb(match *scalar {
                    FieldValueRef::String(s) => s.len() as u64,
                    FieldValueRef::Int32(v) => v as u64,
                    FieldValueRef::Int64(v) => v as u64,
                    FieldValueRef::UInt32(v) => v as u64,
                    FieldValueRef::UInt64(v) => v,
                    FieldValueRef::Bool(v) => v as u64,
                    FieldValueRef::Float(v) => v.to_bits() as u64,
                    FieldValueRef::Double(v) => v.to_bits(),
                    FieldValueRef::Bytes(b) => b.len() as u64,
                }),
                None => bb(0u64),
            };
        }
        for &field_num in &fields.repeated_scalars {
            bb(collected.get_repeated_scalars(field_num).len() as u64);
            total_field_count += 1;
        }
        for &field_num in &fields.repeated_messages {
            bb(collected.get_repeated_messages(field_num).len() as u64);
            total_field_count += 1;
        }
        for &field_num in &fields.maps {
            bb(collected.get_map_entries_count(field_num) as u64);
            total_field_count += 1;
        }
    }
    total_field_count
}

pub fn create_sized_message(
    msg_desc: &MessageDescriptor,
    base_json: &str,
    padding_field: &str,
    target_size: usize,
) -> Vec<u8> {
    let base_encoded = json_to_proto_bytes(msg_desc, base_json);
    let base_size = base_encoded.len();
    if target_size <= base_size {
        return base_encoded;
    }

    let padding_needed = target_size - base_size;
    let mut json_value: JsonValue = serde_json::from_str(base_json).expect("parse base JSON");
    if let Some(field) = json_value.get_mut(padding_field) {
        let current_value = field.as_str().unwrap_or("");
        let padding: String = "x".repeat(padding_needed);
        *field = JsonValue::String(format!("{current_value}{padding}"));
    }
    let padded_json = serde_json::to_string(&json_value).expect("re-serialize padded JSON");
    json_to_proto_bytes(msg_desc, &padded_json)
}

#[allow(dead_code)]
pub fn create_encoded_messages(
    msg_desc: &MessageDescriptor,
    base_json: &str,
    padding_field: &str,
    target_size: usize,
    count: usize,
) -> Vec<Vec<u8>> {
    let message = create_sized_message(msg_desc, base_json, padding_field, target_size);
    vec![message; count]
}

pub fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes}B")
    }
}
