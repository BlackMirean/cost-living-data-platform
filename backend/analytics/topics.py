"""Topic and filtering helpers for the cost-of-living scenario."""

from __future__ import annotations

import re
from collections.abc import Iterable


COST_OF_LIVING_TOPICS = {
    "housing": [
        "rent",
        "rent increase",
        "rent hike",
        "rental",
        "renting",
        "rental crisis",
        "rents",
        "renter",
        "renters",
        "renter rights",
        "landlord",
        "lease",
        "tenant",
        "tenant rights",
        "eviction",
        "evictions",
        "no grounds eviction",
        "no grounds evictions",
        "no-grounds eviction",
        "no-grounds evictions",
        "mortgage",
        "mortgage stress",
        "interest rate",
        "interest rates",
        "rba",
        "housing",
        "housing affordability",
        "housing supply",
        "apartment",
        "house price",
        "house prices",
    ],
    "groceries": [
        "grocery",
        "groceries",
        "grocery prices",
        "supermarket",
        "coles",
        "woolworths",
        "woolies",
        "aldi",
        "food",
        "food prices",
        "price gouging",
        "milk",
        "bread",
    ],
    "fuel": [
        "petrol",
        "petrol prices",
        "fuel",
        "fuel prices",
        "diesel",
        "servo",
        "bowser",
    ],
    "energy": [
        "electricity",
        "electricity bill",
        "electricity prices",
        "energy",
        "power",
        "power bill",
        "power prices",
        "gas bill",
        "energy bill",
        "bill shock",
        "rebate",
        "energy rebate",
        "utility",
        "utilities",
    ],
    "transport": [
        "myki",
        "opal",
        "public transport",
        "train fare",
        "bus fare",
        "tram fare",
        "train",
        "tram",
        "bus",
        "commute",
        "transport",
        "fare",
        "fares",
        "toll",
        "parking",
    ],
    "eating_out": [
        "coffee",
        "cafe",
        "takeaway",
        "ubereats",
        "doordash",
        "restaurant",
        "brunch",
    ],
    "healthcare": [
        "gp",
        "doctor",
        "dentist",
        "medicare",
        "pharmacy",
        "medicine",
        "health insurance",
        "gap fee",
        "bulk billing",
    ],
    "home_goods": [
        "furniture",
        "appliance",
        "appliances",
        "fridge",
        "washing machine",
        "household items",
        "household goods",
        "home goods",
        "sofa",
        "mattress",
        "whitegoods",
        "microwave",
        "dishwasher",
        "dryer",
        "vacuum",
        "bed frame",
    ],
    "inflation": [
        "inflation",
        "cpi",
        "consumer price index",
        "prices",
        "price rise",
        "price rises",
        "price increase",
        "price increases",
        "cost of living",
        "cost-of-living",
        "living costs",
        "rising prices",
        "rising costs",
    ],
    "wages": [
        "wage",
        "wages",
        "wage growth",
        "salary",
        "pay rise",
        "income",
        "real wages",
        "minimum wage",
    ],
    "debt": [
        "hecs",
        "hecs debt",
        "help debt",
        "student debt",
        "credit card",
        "household debt",
        "loan repayment",
        "repayments",
    ],
    "education": [
        "school fees",
        "tuition fees",
        "education costs",
        "uni fees",
        "university fees",
        "childcare fees",
        "textbook costs",
        "school costs",
        "education",
        "school uniforms",
        "stationery costs",
        "daycare fees",
        "school supplies",
    ],
}

TOPIC_LABELS = {
    "housing": "Housing / Rent",
    "groceries": "Groceries",
    "fuel": "Fuel",
    "energy": "Electricity / Utilities",
    "transport": "Transport",
    "eating_out": "Eating Out",
    "healthcare": "Healthcare",
    "home_goods": "Home Goods",
    "education": "Education",
    "inflation": "Inflation",
    "wages": "Wages",
    "debt": "Debt",
    "cost_of_living": "Cost of Living",
}

TOPIC_CPI_ITEM_CANDIDATES = {
    "housing": [
        "Rents",
        "New dwelling purchase by owner-occupiers",
        "Housing",
    ],
    "groceries": [
        "Food and non-alcoholic beverages",
        "Bread and cereal products",
        "Dairy and related products",
        "Fruit and vegetables",
    ],
    "fuel": [
        "Automotive fuel",
        "Transport",
    ],
    "energy": [
        "Electricity",
        "Gas and other household fuels",
    ],
    "transport": [
        "Transport",
        "Urban transport fares",
        "Automotive fuel",
    ],
    "eating_out": [
        "Meals out and take away foods",
        "Food and non-alcoholic beverages",
    ],
    "healthcare": [
        "Medical and hospital services",
        "Pharmaceutical products",
        "Health",
    ],
    "home_goods": [
        "Furniture",
        "Furniture and furnishings",
        "Household equipment and services",
    ],
    "education": [
        "Education",
        "Child care",
        "Tertiary education",
        "Secondary education",
    ],
    "inflation": [
        "All groups CPI",
    ],
    "wages": [
        "All groups CPI",
    ],
    "debt": [
        "All groups CPI",
    ],
    "cost_of_living": [
        "All groups CPI",
    ],
}

AUSTRALIA_LOCATION_TERMS = [
    "Australia",
    "Australian",
    "Australians",
    "Sydney",
    "Melbourne",
    "Brisbane",
    "Perth",
    "Adelaide",
    "Canberra",
    "Hobart",
    "Darwin",
    "Gold Coast",
    "Geelong",
    "Tasmania",
    "NSW",
    "VIC",
    "QLD",
    "WA",
    "SA",
]

AUSTRALIA_CONTEXT_TERMS = {
    "australia",
    "australian",
    "australians",
    "aussie",
    "aus",
    "auspol",
    "melbourne",
    "sydney",
    "brisbane",
    "perth",
    "adelaide",
    "canberra",
    "tasmania",
    "hobart",
    "darwin",
    "gold coast",
    "geelong",
    "nsw",
    "vic",
    "qld",
    "wa",
    "sa",
    "myki",
    "opal",
    "medicare",
    "woolworths",
    "woolies",
    "coles",
    "aldi australia",
    "centrelink",
}

AUSTRALIA_STRONG_CONTEXT_TERMS = {
    "auspol",
    "melbourne",
    "sydney",
    "brisbane",
    "perth",
    "adelaide",
    "canberra",
    "tasmania",
    "hobart",
    "darwin",
    "geelong",
    "nsw",
    "vic",
    "qld",
    "wa",
    "sa",
    "myki",
    "opal",
    "medicare",
    "woolworths",
    "woolies",
    "coles",
    "centrelink",
}

COMPLAINT_TERMS = {
    "expensive",
    "too expensive",
    "cant afford",
    "can't afford",
    "afford",
    "affordability",
    "costs too much",
    "cost too much",
    "costing more",
    "costing too much",
    "more expensive",
    "getting expensive",
    "getting pricier",
    "so expensive",
    "so pricey",
    "pricey",
    "overpriced",
    "price hike",
    "prices up",
    "price rise",
    "price rises",
    "price increase",
    "price increases",
    "gone up",
    "goes up",
    "going up",
    "keep going up",
    "keeps going up",
    "everything is going up",
    "increase",
    "increased",
    "raising",
    "rising",
    "outrageous",
    "ridiculous",
    "crazy prices",
    "out of control",
    "through the roof",
    "gone through the roof",
    "struggling",
    "struggle",
    "struggling with bills",
    "struggling to pay",
    "broke",
    "cost of living",
    "bill shock",
    "tight budget",
    "budget pressure",
    "budget squeeze",
    "rent hike",
    "rent increase",
    "price gouging",
    "stress",
    "cost pressure",
    "financial pressure",
    "hurting",
    "hurts",
    "squeezed",
    "squeeze",
    "unaffordable",
    "hard to pay",
    "hard to keep up",
    "getting harder",
    "living is getting harder",
    "harder to afford",
    "breaking the bank",
    "stretching the budget",
    "too much",
}

GDELT_COVERAGE_TERMS = [
    "cost of living",
    "cost-of-living",
    "affordability",
    "unaffordable",
    "inflation",
    "prices",
    "price rise",
    "price rises",
    "price increase",
    "price increases",
    "bill shock",
    "housing affordability",
    "housing supply",
    "mortgage stress",
    "rental crisis",
    "rent increase",
    "household budgets",
    "household budget",
    "living costs",
    "rising costs",
    "rising prices",
    "crisis",
    "squeeze",
    "squeezed",
]

GENERIC_FOREIGN_TERMS = {
    "england",
    "britain",
    "uk",
    "united kingdom",
    "europe",
    "european",
    "america",
    "american",
    "usa",
    "us consumer",
    "republican",
    "republicans",
    "gop",
    "trump",
    "philippines",
    "sara duterte",
    "social security",
    "new york",
    "canada",
    "ireland",
    "bournemouth",
    "bristol",
    "bay area",
}

KNOWN_NOISE_TEXT = {
    "available in the ios app store",
    "rent free in my head",
}

TOPIC_BLOCKED_TERMS = {
    "housing": {"west end", "musical", "broadway", "show"},
    "fuel": {"war profits", "renewables", "solar", "geopolitics"},
    "home_goods": {"interior design", "home decor", "styling tips", "gift guide", "sale now on"},
    "education": {
        "education policy",
        "curriculum reform",
        "school ranking",
        "teaching strategy",
        "student visa",
    },
}

GDELT_REQUIRE_BODY_KEYWORD_TOPICS = {
    "housing",
    "groceries",
    "fuel",
    "energy",
    "transport",
    "eating_out",
    "healthcare",
    "home_goods",
    "education",
}


def _contains_any(lowered: str, terms: Iterable[str]) -> bool:
    for term in terms:
        cleaned = term.casefold()
        if cleaned.isalnum():
            if re.search(rf"(?<![a-z0-9]){re.escape(cleaned)}(?![a-z0-9])", lowered):
                return True
            continue
        if cleaned in lowered:
            return True
    return False


def _dedupe_terms(terms: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for term in terms:
        lowered = term.casefold()
        if lowered not in seen:
            seen.add(lowered)
            result.append(term)
    return result


def classify_cost_of_living_topic(text: str) -> str:
    """Classify text into a cost-of-living topic."""

    lowered = text.casefold()
    for topic, keywords in COST_OF_LIVING_TOPICS.items():
        if _contains_any(lowered, keywords):
            return topic
    return "cost_of_living"


def matched_keywords(text: str) -> list[str]:
    """Return cost-of-living keywords found in text."""

    lowered = text.casefold()
    matches: list[str] = []
    for keywords in COST_OF_LIVING_TOPICS.values():
        matches.extend(keyword for keyword in keywords if _contains_any(lowered, [keyword]))
    return sorted(set(matches))


def infer_topic_from_query(query: str) -> str | None:
    """Infer the most likely topic for a configured query seed."""

    cleaned = query.casefold().strip().replace("_", " ")
    if cleaned in {
        "cost of living",
        "cost-of-living",
        "living costs",
        "rising costs",
        "rising prices",
    }:
        return None
    for topic in COST_OF_LIVING_TOPICS:
        if cleaned == topic.replace("_", " "):
            return topic

    topic = classify_cost_of_living_topic(query)
    return None if topic == "cost_of_living" else topic


def gdelt_terms_for_topic(topic: str | None) -> list[str]:
    """Return topic terms used to build a richer GDELT search query."""

    if not topic or topic == "cost_of_living":
        return []
    return _dedupe_terms(COST_OF_LIVING_TOPICS.get(topic, []))


def has_australia_context(text: str) -> bool:
    """Return whether text contains an Australia-specific context signal."""

    lowered = text.casefold()
    return _contains_any(lowered, AUSTRALIA_CONTEXT_TERMS) or _contains_any(
        lowered,
        AUSTRALIA_STRONG_CONTEXT_TERMS,
    )


def is_relevant_cost_of_living_text(
    text: str,
    topic_hint: str | None = None,
    *,
    trust_topic_hint: bool = False,
) -> bool:
    """Return whether a candidate article looks relevant enough to index."""

    if not text.strip():
        return False

    lowered = text.casefold()
    if _contains_any(lowered, KNOWN_NOISE_TEXT):
        return False

    classified_topic = classify_cost_of_living_topic(text)
    if topic_hint and _contains_any(lowered, COST_OF_LIVING_TOPICS.get(topic_hint, [])):
        topic = topic_hint
    else:
        topic = classified_topic
    blocked_terms = TOPIC_BLOCKED_TERMS.get(topic, set())
    if _contains_any(lowered, blocked_terms):
        return False

    has_topic_keyword = topic != "cost_of_living" and _contains_any(
        lowered,
        COST_OF_LIVING_TOPICS.get(topic, []),
    )
    has_cost_signal = _contains_any(lowered, GDELT_COVERAGE_TERMS) or _contains_any(
        lowered,
        COMPLAINT_TERMS,
    )
    has_australia_signal = _contains_any(lowered, AUSTRALIA_CONTEXT_TERMS)
    has_strong_australia_signal = _contains_any(lowered, AUSTRALIA_STRONG_CONTEXT_TERMS)
    has_foreign_signal = _contains_any(lowered, GENERIC_FOREIGN_TERMS)

    if has_foreign_signal and not (has_australia_signal or has_strong_australia_signal):
        return False

    if topic in GDELT_REQUIRE_BODY_KEYWORD_TOPICS:
        if trust_topic_hint and topic_hint and topic == topic_hint:
            return has_topic_keyword
        return has_topic_keyword and has_cost_signal

    return has_topic_keyword or has_cost_signal


def cpi_items_for_topic(topic: str) -> list[str]:
    """Return CPI item candidates that best match a social/news topic."""

    return TOPIC_CPI_ITEM_CANDIDATES.get(topic, TOPIC_CPI_ITEM_CANDIDATES["cost_of_living"])
