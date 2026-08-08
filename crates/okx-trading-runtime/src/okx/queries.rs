use url::form_urlencoded::Serializer;

pub(crate) const OKX_ORDER_HISTORY_PAGE_LIMIT: usize = 100;
pub(crate) const OKX_OPEN_ORDERS_PAGE_LIMIT: usize = 100;
pub(crate) const OKX_ORDER_FILLS_PAGE_LIMIT: usize = 100;
pub(crate) const OKX_OPEN_ALGO_ORDERS_PAGE_LIMIT: usize = 100;
pub(crate) const OKX_ALGO_HISTORY_PAGE_LIMIT: usize = 100;

pub(crate) fn okx_query(pairs: &[(&str, &str)]) -> String {
    let mut serializer = Serializer::new(String::new());
    for (name, value) in pairs {
        serializer.append_pair(name, value);
    }
    serializer.finish()
}

pub(crate) fn open_orders_query(inst_type: &str, inst_id: &str, after: Option<&str>) -> String {
    let limit = OKX_OPEN_ORDERS_PAGE_LIMIT.to_string();
    let mut pairs = vec![
        ("instType", inst_type),
        ("instId", inst_id),
        ("limit", &limit),
    ];
    pairs.extend(after.map(|after| ("after", after)));
    okx_query(&pairs)
}

pub(crate) fn order_history_query(inst_type: &str, inst_id: &str, after: Option<&str>) -> String {
    let limit = OKX_ORDER_HISTORY_PAGE_LIMIT.to_string();
    let mut pairs = vec![
        ("instType", inst_type),
        ("instId", inst_id),
        ("limit", &limit),
    ];
    pairs.extend(after.map(|after| ("after", after)));
    okx_query(&pairs)
}

pub(crate) fn order_fills_query(inst_type: &str, inst_id: &str, after: Option<&str>) -> String {
    let limit = OKX_ORDER_FILLS_PAGE_LIMIT.to_string();
    let mut pairs = vec![
        ("instType", inst_type),
        ("instId", inst_id),
        ("limit", &limit),
    ];
    pairs.extend(after.map(|after| ("after", after)));
    okx_query(&pairs)
}

pub(crate) fn open_algo_orders_query(
    inst_type: &str,
    inst_id: &str,
    after: Option<&str>,
) -> String {
    let limit = OKX_OPEN_ALGO_ORDERS_PAGE_LIMIT.to_string();
    let mut pairs = vec![
        ("instType", inst_type),
        ("instId", inst_id),
        ("ordType", "trigger"),
        ("limit", &limit),
    ];
    pairs.extend(after.map(|after| ("after", after)));
    okx_query(&pairs)
}

#[derive(Clone, Copy)]
pub(crate) enum AlgoHistoryFilter<'a> {
    State(&'a str),
    AlgoId(&'a str),
}

pub(crate) fn algo_order_history_query(
    inst_type: &str,
    inst_id: &str,
    filter: AlgoHistoryFilter<'_>,
    after: Option<&str>,
) -> String {
    let limit = OKX_ALGO_HISTORY_PAGE_LIMIT.to_string();
    let mut pairs = vec![
        ("instType", inst_type),
        ("instId", inst_id),
        ("ordType", "trigger"),
    ];
    match filter {
        AlgoHistoryFilter::State(state) => {
            pairs.push(("state", state));
        }
        AlgoHistoryFilter::AlgoId(algo_id) => {
            pairs.push(("algoId", algo_id));
        }
    }
    pairs.push(("limit", &limit));
    pairs.extend(after.map(|after| ("after", after)));
    okx_query(&pairs)
}
