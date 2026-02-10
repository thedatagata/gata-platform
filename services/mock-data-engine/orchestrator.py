"""
Mock Data Orchestrator — User-First Simulation Engine

Merges the dlt pipeline wiring (old orchestrator) with the event-driven
funnel simulation (user_simulation) into a single file.

Flow:
    Phase 1 (Reference Data):
        - Run paid ad generators as-is → extract campaign names for UTM pools
        - Run ecommerce generators as-is → extract product catalog
        - Generators are unchanged; their output is consumed as reference data

    Phase 2 (Simulation):
        - Spawn anonymous users with cookie IDs
        - Simulate sessions with UTMs pulled from ad generator campaign data
        - Walk funnel with probability gates driven by tenants.yaml funnel config
        - Generate orders from product catalog when users reach purchase
        - Returning customers get boosted funnel probabilities (brand familiarity)

    Phase 3 (Loading):
        - Format simulation outputs into each platform's native schema
        - Pass ad generator output through unchanged
        - Load everything via dlt pipeline

User archetypes (bouncer, browser, single-purchaser, repeat-purchaser)
emerge naturally from the probability mechanics — they are NOT pre-assigned.
"""
import dlt
import polars as pl
import random
import uuid
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional, Tuple
from faker import Faker

from sources.digital_analytics.shared_config import (
    SEED, FUNNEL_EVENTS,
    INTRA_SESSION_DELAY_SECONDS, INTER_SESSION_DELAY_SECONDS,
    pick_weighted,
)

# --- Generator Imports (unchanged) ---
from sources.paid_ads.facebook_ads.fb_ads_data_generator import generate_facebook_data
from sources.paid_ads.google_ads.google_ads_data_generator import generate_google_ads
from sources.paid_ads.tiktok_ads.tiktok_ads_data_generator import generate_tiktok_data
from sources.paid_ads.instagram_ads.ig_ads_data_generator import generate_instagram_data
from sources.paid_ads.linkedin_ads.linkedin_ads_data_generator import generate_linkedin_data
from sources.paid_ads.bing_ads.bing_ads_data_generator import generate_bing_data
from sources.paid_ads.amazon_ads.amazon_ads_data_generator import generate_amazon_data
from sources.ecommerce_platforms.shopify.shopify_data_generator import generate_shopify_data
from sources.ecommerce_platforms.woocommerce.woocommerce_data_generator import generate_woocommerce_data
from sources.ecommerce_platforms.bigcommerce.bigcommerce_data_generator import generate_bigcommerce_data


# ═══════════════════════════════════════════════════════════════
# ORCHESTRATOR CLASS (dlt pipeline wiring)
# ═══════════════════════════════════════════════════════════════

class MockOrchestrator:
    def __init__(self, config: Any, days: int = 90, credentials: str = None):
        """Initializes with a default 90-day window for categorical density."""
        self.config = config
        self.days = days
        self.credentials = credentials

    def run(self) -> Dict[str, Any]:
        """Orchestrates data generation via simulation, then loads via dlt."""

        # ── dlt pipeline setup (unchanged) ──
        is_local = self.credentials and 'duckdb' in self.credentials
        if is_local:
            from pathlib import Path
            sandbox_path = str(Path(__file__).resolve().parent.parent.parent / "warehouse" / "sandbox.duckdb")
            destination = dlt.destinations.duckdb(credentials=sandbox_path)
        else:
            destination = 'motherduck'

        pipeline = dlt.pipeline(
            pipeline_name=f'mock_load_{self.config.slug}',
            destination=destination,
            dataset_name=self.config.slug,
        )

        load_package = []

        def create_table_etl(source_name: str, table_name: str, raw_data: List[dict]):
            """Chains Extract, Transform, and Load stages for physical schema stability."""
            full_table_name = f"raw_{self.config.slug}_{source_name}_{table_name}"

            @dlt.resource(name=f"{full_table_name}_extract", selected=False)
            def extract():
                yield pl.DataFrame(raw_data)

            @dlt.transformer(data_from=extract, name=f"{full_table_name}_transform", selected=False)
            def transform(df: pl.DataFrame):
                dt_cols = [c for c, t in df.schema.items() if isinstance(t, (pl.Datetime, pl.Date))]
                if dt_cols:
                    df = df.with_columns([pl.col(c).cast(pl.String) for c in dt_cols])
                return df

            @dlt.transformer(data_from=transform, name=full_table_name, max_table_nesting=0)
            def load(df: pl.DataFrame):
                yield df.to_arrow()

            return load

        # ── PHASE 1: Run existing generators for reference data ──

        sources = self.config.sources
        slug = self.config.slug

        # 1a. Paid ads — run as-is, collect output + extract campaign names
        ad_outputs, campaign_pool = _run_ad_generators(slug, sources, self.days)

        # Load ad generator output into dlt (pass-through, no simulation changes)
        for platform_name, tables in ad_outputs.items():
            for table_name, rows in tables.items():
                load_package.append(create_table_etl(platform_name, table_name, rows))

        # 1b. Ecommerce — run for products only (orders come from simulation)
        ecom_platform, ecom_products = _run_ecommerce_for_products(slug, sources, self.days)

        # ── PHASE 2: Simulate user funnel behavior ──

        funnel_config = _get_funnel_config(sources, ecom_platform)

        sim = _run_simulation(
            tenant_slug=slug,
            days=self.days,
            funnel_config=funnel_config,
            campaign_pool=campaign_pool,
            product_catalog=ecom_products,
        )

        # ── PHASE 3: Format simulation output + load ──

        # 3a. Ecommerce — products from generator + orders from simulation
        if ecom_platform:
            ecom_tables = _format_ecommerce(ecom_platform, ecom_products, sim)
            for table_name, rows in ecom_tables.items():
                load_package.append(create_table_etl(ecom_platform, table_name, rows))

        # 3b. Analytics — events/sessions/users from simulation
        if sources.google_analytics.enabled:
            ga4_tables = _format_ga4(sim)
            for table_name, rows in ga4_tables.items():
                load_package.append(create_table_etl('google_analytics', table_name, rows))

        if sources.amplitude.enabled:
            amp_tables = _format_amplitude(sim)
            for table_name, rows in amp_tables.items():
                load_package.append(create_table_etl('amplitude', table_name, rows))

        if sources.mixpanel.enabled:
            mp_tables = _format_mixpanel(sim)
            for table_name, rows in mp_tables.items():
                load_package.append(create_table_etl('mixpanel', table_name, rows))

        # ── Atomic dlt load ──
        if load_package:
            pipeline.run(load_package)

        return pipeline.default_schema.to_dict()


# ═══════════════════════════════════════════════════════════════
# PHASE 1: REFERENCE DATA FROM EXISTING GENERATORS
# ═══════════════════════════════════════════════════════════════

_AD_GENERATORS = {
    "facebook_ads":  ("facebook_ads",  generate_facebook_data),
    "google_ads":    ("google_ads",    generate_google_ads),
    "instagram_ads": ("instagram_ads", generate_instagram_data),
    "linkedin_ads":  ("linkedin_ads",  generate_linkedin_data),
    "bing_ads":      ("bing_ads",      generate_bing_data),
    "tiktok_ads":    ("tiktok_ads",    generate_tiktok_data),
    "amazon_ads":    ("amazon_ads",    generate_amazon_data),
}

# Source/medium mapping for each ad platform
_PLATFORM_TRAFFIC = {
    "google_ads":    {"source": "google",    "medium": "cpc"},
    "facebook_ads":  {"source": "facebook",  "medium": "cpc"},
    "instagram_ads": {"source": "instagram", "medium": "cpc"},
    "bing_ads":      {"source": "bing",      "medium": "cpc"},
    "linkedin_ads":  {"source": "linkedin",  "medium": "cpc"},
    "tiktok_ads":    {"source": "tiktok",    "medium": "cpc"},
    "amazon_ads":    {"source": "amazon",    "medium": "cpc"},
}

# Organic traffic sources (not tied to any ad platform)
_ORGANIC_TRAFFIC = [
    {"source": "google",    "medium": "organic",  "weight": 0.40},
    {"source": "(direct)",  "medium": "(none)",    "weight": 0.30},
    {"source": "email",     "medium": "email",     "weight": 0.15},
    {"source": "referral",  "medium": "referral",  "weight": 0.15},
]


def _run_ad_generators(
    slug: str, sources: Any, days: int,
) -> Tuple[Dict[str, Dict[str, List]], Dict[str, List[str]]]:
    """
    Run all enabled ad generators. Returns:
        ad_outputs:    {platform → {table → [rows]}} for dlt loading
        campaign_pool: {platform → [campaign_name, ...]} for UTM assignment
    """
    ad_outputs = {}
    campaign_pool: Dict[str, List[str]] = {}

    for platform_name, (attr_name, gen_func) in _AD_GENERATORS.items():
        source_cfg = getattr(sources, attr_name, None)
        if source_cfg and source_cfg.enabled:
            raw = gen_func(slug, source_cfg.generation, days)
            ad_outputs[platform_name] = raw

            # Extract campaign names from generator output
            campaigns = raw.get("campaigns", [])
            names = [
                c.get("name") or c.get("campaign_name") or ""
                for c in campaigns
            ]
            names = [n for n in names if n]
            if names:
                campaign_pool[platform_name] = names

    return ad_outputs, campaign_pool


def _run_ecommerce_for_products(
    slug: str, sources: Any, days: int,
) -> Tuple[Optional[str], List[Dict]]:
    """
    Run the enabled ecommerce generator to extract the product catalog.
    Returns (platform_name, products_list).
    Orders will be replaced by simulation output.
    """
    ecom_map = {
        "shopify":      (sources.shopify,      generate_shopify_data),
        "woocommerce":  (sources.woocommerce,  generate_woocommerce_data),
        "bigcommerce":  (sources.bigcommerce,  generate_bigcommerce_data),
    }
    for platform_name, (source_cfg, gen_func) in ecom_map.items():
        if source_cfg.enabled:
            raw = gen_func(slug, source_cfg.generation, days)
            products = raw.get("products", [])
            return platform_name, products
    return None, []


def _get_funnel_config(sources: Any, ecom_platform: Optional[str]) -> Any:
    """
    Get the FunnelConfig from the appropriate source's generation block.
    Checks ecommerce first, then analytics, then falls back to defaults.
    """
    # Try ecommerce source config
    if ecom_platform:
        source_cfg = getattr(sources, ecom_platform, None)
        if source_cfg and hasattr(source_cfg.generation, 'funnel'):
            return source_cfg.generation.funnel

    # Try analytics source config
    for attr in ['google_analytics', 'amplitude', 'mixpanel']:
        source_cfg = getattr(sources, attr, None)
        if source_cfg and source_cfg.enabled and hasattr(source_cfg.generation, 'funnel'):
            return source_cfg.generation.funnel

    # Default — will use FunnelConfig() defaults
    from config import FunnelConfig
    return FunnelConfig()


# ═══════════════════════════════════════════════════════════════
# PHASE 2: EVENT-DRIVEN FUNNEL SIMULATION
# ═══════════════════════════════════════════════════════════════

class SimulationResult:
    """Raw simulation output before platform-specific formatting."""
    __slots__ = (
        "users", "sessions", "events", "orders", "products",
        "user_index", "session_index",
    )
    def __init__(self):
        self.users: List[Dict] = []
        self.sessions: List[Dict] = []
        self.events: List[Dict] = []
        self.orders: List[Dict] = []
        self.products: List[Dict] = []
        self.user_index: Dict[str, Dict] = {}
        self.session_index: Dict[str, Dict] = {}


def _run_simulation(
    tenant_slug: str,
    days: int,
    funnel_config: Any,
    campaign_pool: Dict[str, List[str]],
    product_catalog: List[Dict],
) -> SimulationResult:
    """
    Core simulation loop.

    Funnel probabilities come from tenants.yaml via FunnelConfig.
    Campaign names come from ad generator output.
    Products come from ecommerce generator output.

    For each user:
        1. Random first-visit time within date range
        2. Session loop:
           - session_start always fires
           - Probability gate at each funnel step (low→high)
           - If advances: next event in 1-30 min
           - If drops off: return probability based on depth reached
           - If returns: next session in 31 min - 7 days
        3. On purchase: create order from product catalog
        4. Returning customers get boosted funnel probabilities
    """
    rng = random.Random(SEED)
    fake = Faker()
    Faker.seed(SEED)

    result = SimulationResult()
    result.products = product_catalog

    # Resolve funnel probabilities from tenant config
    advance_rates = funnel_config.get_advance_rates()
    return_rates = funnel_config.get_return_rates()
    customer_boost = funnel_config.returning_customer_boost

    # Build traffic source pool from enabled ad platforms + organic
    traffic_sources = _build_traffic_sources(campaign_pool)

    # Size user pool to hit target order volume
    # ~6.5% of initial users will eventually produce an order
    daily_orders_target = 15.0  # TODO: pull from generation config
    total_orders_target = int(daily_orders_target * days)
    total_users = max(int(total_orders_target / 0.065), 200)

    # Date boundaries
    end_dt = datetime.combine(date.today(), datetime.min.time())
    start_dt = end_dt - timedelta(days=days)

    for _ in range(total_users):
        user = _create_user(rng)
        result.users.append(user)
        result.user_index[user["user_id"]] = user

        first_offset = rng.randint(0, max(1, days * 86400 - 1))
        current_time = start_dt + timedelta(seconds=first_offset)

        session_count = 0
        has_more_sessions = True

        while has_more_sessions and current_time < end_dt:
            session_count += 1

            traffic = _pick_traffic(traffic_sources, rng)
            session = _create_session(user, session_count, current_time, traffic, rng)
            result.sessions.append(session)
            result.session_index[session["session_id"]] = session

            # ── Funnel walk ──
            event_time = current_time
            max_depth = 0

            # session_start always fires (step 0)
            result.events.append(_create_event(
                session, user, FUNNEL_EVENTS[0], event_time, 0, None, rng,
            ))

            browsed_product = rng.choice(product_catalog) if product_catalog else None

            for step_idx in range(1, len(FUNNEL_EVENTS)):
                from_event = FUNNEL_EVENTS[step_idx - 1]
                to_event = FUNNEL_EVENTS[step_idx]
                rate_key = f"{from_event}_to_{to_event}"

                base_prob = advance_rates.get(rate_key, 0.0)

                # Boost for returning customers
                if user["is_customer"]:
                    base_prob = min(base_prob * customer_boost, 0.95)

                if rng.random() < base_prob:
                    lo, hi = INTRA_SESSION_DELAY_SECONDS
                    event_time += timedelta(seconds=rng.randint(lo, hi))
                    max_depth = step_idx

                    result.events.append(_create_event(
                        session, user, to_event, event_time,
                        step_idx, browsed_product, rng,
                    ))

                    if to_event == "purchase" and product_catalog:
                        order = _create_order(
                            session, user, event_time,
                            product_catalog, fake, rng,
                        )
                        result.orders.append(order)
                        session["converted"] = True
                        user["is_customer"] = True
                        user["order_count"] = user.get("order_count", 0) + 1
                        if not user["email"]:
                            user["email"] = f"{user['user_id'][:12]}@{fake.free_email_domain()}"
                else:
                    break

            session["max_funnel_depth"] = max_depth

            # Return decision
            return_prob = return_rates.get(max_depth, 0.06)
            if rng.random() < return_prob:
                lo, hi = INTER_SESSION_DELAY_SECONDS
                current_time = event_time + timedelta(seconds=rng.randint(lo, hi))
            else:
                has_more_sessions = False

    _print_summary(result, days, tenant_slug)
    return result


# ═══════════════════════════════════════════════════════════════
# TRAFFIC SOURCE RESOLUTION
# ═══════════════════════════════════════════════════════════════

def _build_traffic_sources(campaign_pool: Dict[str, List[str]]) -> List[Dict]:
    """
    Build traffic source pool from enabled ad platform campaigns + organic.
    Each paid source gets weight proportional to its campaign count.
    """
    sources = []
    total_campaigns = sum(len(v) for v in campaign_pool.values())
    paid_weight_total = 0.58  # ~58% of traffic from paid

    for platform, campaigns in campaign_pool.items():
        traffic_info = _PLATFORM_TRAFFIC.get(platform, {})
        if not traffic_info:
            continue
        platform_weight = (len(campaigns) / max(total_campaigns, 1)) * paid_weight_total
        sources.append({
            "source": traffic_info["source"],
            "medium": traffic_info["medium"],
            "weight": platform_weight,
            "is_paid": True,
            "platform": platform,
            "campaigns": campaigns,
        })

    organic_weight_total = 1.0 - sum(s["weight"] for s in sources)
    for organic in _ORGANIC_TRAFFIC:
        sources.append({
            "source": organic["source"],
            "medium": organic["medium"],
            "weight": organic["weight"] * organic_weight_total,
            "is_paid": False,
            "platform": None,
            "campaigns": [],
        })

    return sources


def _pick_traffic(sources: List[Dict], rng: random.Random) -> Dict:
    """Pick a traffic source and resolve UTM campaign name."""
    selected = pick_weighted(sources, rng=rng)
    result = dict(selected)
    if result["is_paid"] and result["campaigns"]:
        result["utm_campaign"] = rng.choice(result["campaigns"])
    else:
        result["utm_campaign"] = "(not set)"
    return result


# ═══════════════════════════════════════════════════════════════
# RECORD CREATORS
# ═══════════════════════════════════════════════════════════════

_DEVICE_WEIGHTS = [
    {"device": "mobile",  "weight": 0.55},
    {"device": "desktop", "weight": 0.35},
    {"device": "tablet",  "weight": 0.10},
]

_GEO_WEIGHTS = [
    {"country": "US", "cities": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"], "weight": 0.50},
    {"country": "CA", "cities": ["Toronto", "Vancouver", "Montreal"], "weight": 0.12},
    {"country": "GB", "cities": ["London", "Manchester", "Birmingham"], "weight": 0.10},
    {"country": "DE", "cities": ["Berlin", "Munich", "Hamburg"], "weight": 0.08},
    {"country": "AU", "cities": ["Sydney", "Melbourne", "Brisbane"], "weight": 0.07},
    {"country": "FR", "cities": ["Paris", "Lyon", "Marseille"], "weight": 0.06},
    {"country": "JP", "cities": ["Tokyo", "Osaka", "Yokohama"], "weight": 0.04},
    {"country": "BR", "cities": ["Sao Paulo", "Rio de Janeiro"], "weight": 0.03},
]

_LANDING_PAGES_PAID = [
    "/landing/sale", "/landing/new-arrivals", "/landing/free-shipping",
    "/products/featured", "/collections/best-sellers",
]
_LANDING_PAGES_ORGANIC = [
    "/", "/products", "/collections/all", "/about", "/blog/post",
]


def _create_user(rng: random.Random) -> Dict:
    """Spawn an anonymous user with a cookie ID. No email until they purchase."""
    device = pick_weighted(_DEVICE_WEIGHTS, rng=rng)
    geo = pick_weighted(_GEO_WEIGHTS, rng=rng)
    return {
        "user_id": f"u_{uuid.uuid4().hex[:12]}",
        "cookie_id": f"GA1.1.{rng.randint(1_000_000,9_999_999)}.{rng.randint(1_600_000_000,1_700_000_000)}",
        "email": "",
        "device_category": device["device"],
        "geo_country": geo["country"],
        "geo_city": rng.choice(geo["cities"]),
        "is_customer": False,
        "order_count": 0,
    }


def _create_session(
    user: Dict, session_number: int, timestamp: datetime,
    traffic: Dict, rng: random.Random,
) -> Dict:
    """Create a session record with UTM attribution from campaign data."""
    pages = _LANDING_PAGES_PAID if traffic["is_paid"] else _LANDING_PAGES_ORGANIC
    return {
        "session_id": f"s_{uuid.uuid4().hex[:16]}",
        "user_id": user["user_id"],
        "cookie_id": user["cookie_id"],
        "session_number": session_number,
        "timestamp": timestamp,
        "utm_source": traffic["source"],
        "utm_medium": traffic["medium"],
        "utm_campaign": traffic.get("utm_campaign", "(not set)"),
        "landing_page": rng.choice(pages),
        "device_category": user["device_category"],
        "geo_country": user["geo_country"],
        "geo_city": user["geo_city"],
        "is_paid": traffic["is_paid"],
        "converted": False,
        "max_funnel_depth": 0,
    }


def _create_event(
    session: Dict, user: Dict, event_name: str,
    timestamp: datetime, step_index: int,
    product: Optional[Dict], rng: random.Random,
) -> Dict:
    """Create a single funnel event."""
    return {
        "event_id": f"e_{uuid.uuid4().hex[:16]}",
        "session_id": session["session_id"],
        "user_id": user["user_id"],
        "cookie_id": user["cookie_id"],
        "event_name": event_name,
        "timestamp": timestamp,
        "event_index": step_index,
        "product_id": product.get("product_id", product.get("id")) if product and step_index >= 1 else None,
        "product_price": product["price"] if product and step_index >= 1 else None,
        "product_category": product.get("category") if product and step_index >= 1 else None,
    }


def _create_order(
    session: Dict, user: Dict, purchase_time: datetime,
    products: List[Dict], fake: Faker, rng: random.Random,
) -> Dict:
    """Create an ecommerce order from a purchase event."""
    n_items = rng.randint(1, 4)
    selected = rng.choices(products, k=n_items)
    line_items = []
    subtotal = 0.0
    for prod in selected:
        qty = rng.randint(1, 3)
        item_total = round(prod["price"] * qty, 2)
        subtotal += item_total
        line_items.append({
            "product_id": prod.get("product_id", prod.get("id")),
            "title": prod.get("title", prod.get("name", "")),
            "sku": prod.get("sku", ""),
            "quantity": qty,
            "price": prod["price"],
            "total": item_total,
        })

    email = user.get("email") or f"{user['user_id'][:12]}@{fake.free_email_domain()}"
    user["email"] = email

    return {
        "order_id": rng.randint(100_000, 9_999_999),
        "user_id": user["user_id"],
        "cookie_id": user["cookie_id"],
        "session_id": session["session_id"],
        "timestamp": purchase_time,
        "total_price": round(subtotal, 2),
        "currency": "USD",
        "financial_status": "paid",
        "fulfillment_status": rng.choice(["fulfilled", "unfulfilled", "partial"]),
        "customer_email": email,
        "customer_id": user["user_id"],
        "line_items": line_items,
        "utm_source": session["utm_source"],
        "utm_medium": session["utm_medium"],
        "utm_campaign": session["utm_campaign"],
    }


# ═══════════════════════════════════════════════════════════════
# PHASE 3: FORMAT SIMULATION OUTPUT PER PLATFORM SCHEMA
# ═══════════════════════════════════════════════════════════════
# Each formatter produces the same {table_name: [rows]} shape
# that the original generators returned, so dlt loading is unchanged.

def _format_ecommerce(
    platform: str, products: List[Dict], sim: SimulationResult,
) -> Dict[str, List[dict]]:
    """Route to the correct ecommerce formatter."""
    formatters = {
        "shopify": _format_shopify,
        "woocommerce": _format_woocommerce,
        "bigcommerce": _format_bigcommerce,
    }
    formatter = formatters.get(platform)
    if formatter:
        return formatter(products, sim)
    return {}


def _format_shopify(products: List[Dict], sim: SimulationResult) -> Dict[str, List[dict]]:
    """Match the shape of generate_shopify_data output."""
    orders = []
    for o in sim.orders:
        shopify_line_items = []
        for li in o["line_items"]:
            shopify_line_items.append({
                "id": random.randint(1000, 9999),
                "product_id": hash(li["product_id"]) % 900000 + 100000,
            })
        orders.append({
            "id": o["order_id"],
            "name": f"#{o['order_id'] % 100000}",
            "email": o["customer_email"],
            "total_price": o["total_price"],
            "currency": o["currency"],
            "financial_status": o["financial_status"],
            "status": o["fulfillment_status"],
            "customer_id": hash(o["customer_id"]) % 1000,
            "customer_email": o["customer_email"],
            "created_at": o["timestamp"],
            "line_items": shopify_line_items,
        })

    shopify_products = []
    for p in products:
        shopify_products.append({
            "id": hash(p.get("product_id", p.get("id", ""))) % 900000 + 100000,
            "title": p.get("title", p.get("name", "")),
            "price": p.get("price", 0),
            "created_at": datetime.now(),
        })

    return {"products": shopify_products, "orders": orders}


def _format_woocommerce(products: List[Dict], sim: SimulationResult) -> Dict[str, List[dict]]:
    """Match the shape of generate_woocommerce_data output."""
    orders = []
    for o in sim.orders:
        woo_line_items = []
        for li in o["line_items"]:
            woo_line_items.append({
                "product_id": hash(li["product_id"]) % 90000 + 1000,
                "name": li["title"],
                "quantity": li["quantity"],
                "price": li["price"],
            })
        orders.append({
            "id": o["order_id"] % 100000,
            "number": str(o["order_id"]),
            "status": "completed" if o["financial_status"] == "paid" else "processing",
            "total_price": o["total_price"],
            "currency": o["currency"],
            "customer_id": hash(o["customer_id"]) % 1000,
            "billing_email": o["customer_email"],
            "line_items": woo_line_items,
            "created_at": o["timestamp"],
        })

    woo_products = []
    for p in products:
        woo_products.append({
            "id": hash(p.get("product_id", p.get("id", ""))) % 90000 + 1000,
            "name": p.get("title", p.get("name", "")),
            "price": p.get("price", 0),
            "created_at": datetime.now(),
        })

    return {"products": woo_products, "orders": orders}


def _format_bigcommerce(products: List[Dict], sim: SimulationResult) -> Dict[str, List[dict]]:
    """Match the shape of generate_bigcommerce_data output."""
    status_map = {True: (11, "Completed"), False: (2, "Shipped")}
    orders = []
    for o in sim.orders:
        is_paid = o["financial_status"] == "paid"
        sid, sname = status_map[is_paid]
        orders.append({
            "id": o["order_id"] % 200000 + 90000,
            "status_id": sid,
            "status": sname,
            "total_price": o["total_price"],
            "currency": o["currency"],
            "customer_id": hash(o["customer_id"]) % 1500 + 500,
            "created_at": o["timestamp"],
        })

    bc_products = []
    for p in products:
        bc_products.append({
            "id": hash(p.get("product_id", p.get("id", ""))) % 90000 + 20000,
            "name": p.get("title", p.get("name", "")),
            "price": p.get("price", 0),
        })

    return {"products": bc_products, "orders": orders}


def _format_ga4(sim: SimulationResult) -> Dict[str, List[dict]]:
    """
    Format simulation events into GA4 schema.
    Matches the shape of generate_ga4_data output with dense defaults.
    """
    ga4_events = []
    for evt in sim.events:
        session = sim.session_index.get(evt["session_id"], {})

        # Match transaction data for purchase events
        order_match = None
        if evt["event_name"] == "purchase":
            for o in sim.orders:
                if o["session_id"] == evt["session_id"]:
                    order_match = o
                    break

        ga4_events.append({
            "event_name": evt["event_name"],
            "event_date": evt["timestamp"].strftime("%Y%m%d"),
            "event_timestamp": int(evt["timestamp"].timestamp() * 1_000_000),
            "user_pseudo_id": f"ga_{evt['cookie_id']}",
            "user_id": evt["user_id"],
            "geo_country": session.get("geo_country", "US"),
            "geo_city": session.get("geo_city", "Unknown"),
            "traffic_source_source": session.get("utm_source", "(direct)"),
            "traffic_source_medium": session.get("utm_medium", "(none)"),
            "traffic_source_campaign": session.get("utm_campaign", "(not set)"),
            "device_category": session.get("device_category", "mobile"),
            "ga_session_id": evt["session_id"][-8:],
            "ecommerce_transaction_id": str(order_match["order_id"]) if order_match else "N/A",
            "ecommerce_value": float(order_match["total_price"]) if order_match else 0.0,
            "ecommerce_currency": "USD",
        })

    return {"events": ga4_events}


def _format_amplitude(sim: SimulationResult) -> Dict[str, List[dict]]:
    """Format simulation into Amplitude events + users schema."""
    amp_events = []
    for evt in sim.events:
        user = sim.user_index.get(evt["user_id"], {})
        amp_events.append({
            "event_id": evt["event_id"],
            "event_type": evt["event_name"],
            "user_id": evt["user_id"],
            "event_time": evt["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
            "device_type": user.get("device_category", "Unknown"),
            "country": user.get("geo_country", "N/A"),
        })

    amp_users = []
    for user in sim.users:
        amp_users.append({
            "user_id": user["user_id"],
            "device_type": user["device_category"],
            "country": user["geo_country"],
        })

    return {"events": amp_events, "users": amp_users}


def _format_mixpanel(sim: SimulationResult) -> Dict[str, List[dict]]:
    """Format simulation into Mixpanel events + people schema."""
    mp_events = []
    for evt in sim.events:
        session = sim.session_index.get(evt["session_id"], {})
        user = sim.user_index.get(evt["user_id"], {})
        mp_events.append({
            "event": evt["event_name"],
            "prop_distinct_id": evt["user_id"],
            "prop_time": int(evt["timestamp"].timestamp()),
            "prop_browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            "prop_city": user.get("geo_city", ""),
            "prop_country_code": user.get("geo_country", ""),
            "prop_device_type": user.get("device_category", ""),
            "prop_utm_source": session.get("utm_source"),
            "prop_utm_medium": session.get("utm_medium"),
            "prop_utm_campaign": session.get("utm_campaign"),
        })

    mp_people = []
    for user in sim.users:
        if user.get("email"):
            mp_people.append({
                "distinct_id": user["user_id"],
                "city": user["geo_city"],
                "email": user["email"],
            })

    return {"events": mp_events, "people": mp_people}


# ═══════════════════════════════════════════════════════════════
# DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════

def _print_summary(result: SimulationResult, days: int, tenant_slug: str):
    """Print funnel diagnostics to validate the simulation."""
    from collections import Counter

    n_users = len(result.users)
    n_sessions = len(result.sessions)
    n_events = len(result.events)
    n_orders = len(result.orders)

    customers = [u for u in result.users if u["is_customer"]]
    repeat = [u for u in customers if u["order_count"] > 1]

    depth_counts: Dict[int, int] = {}
    for s in result.sessions:
        d = s.get("max_funnel_depth", 0)
        depth_counts[d] = depth_counts.get(d, 0) + 1

    cumulative = {}
    running = n_sessions
    for i in range(len(FUNNEL_EVENTS)):
        cumulative[i] = running
        running -= depth_counts.get(i, 0)

    print(f"\n{'='*70}")
    print(f"  SIMULATION: {tenant_slug} | {days} days | {n_users:,} users")
    print(f"{'='*70}")
    print(f"  Sessions: {n_sessions:,}  |  Events: {n_events:,}  |  Orders: {n_orders:,}")
    print(f"  Customers: {len(customers):,}  |  Repeat buyers: {len(repeat):,}  |  Avg orders/day: {n_orders / max(days, 1):.1f}")
    print()
    print(f"  {'Step':<25s} {'Reached':>8s} {'% of top':>9s} {'Dropped':>8s} {'Drop %':>7s}")
    print(f"  {'-'*60}")
    for i, event_name in enumerate(FUNNEL_EVENTS):
        reached = cumulative[i]
        pct_of_top = reached / n_sessions * 100
        dropped = depth_counts.get(i, 0)
        drop_pct = dropped / max(reached, 1) * 100
        bar = "#" * int(pct_of_top / 2)
        print(f"  {event_name:<25s} {reached:>8,} {pct_of_top:>8.1f}% {dropped:>8,} {drop_pct:>6.1f}%  {bar}")

    order_dist = Counter(u["order_count"] for u in customers)
    if order_dist:
        print(f"\n  Order count distribution:")
        for n_ord in sorted(order_dist.keys()):
            print(f"    {n_ord} orders: {order_dist[n_ord]:,} customers")
    print(f"{'='*70}\n")
