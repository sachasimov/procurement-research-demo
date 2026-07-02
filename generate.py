"""Run one Linkup research job (reasoningDepth=L) per procurement/supply-chain
use case and save each result as a shareable markdown file.

Usage (from the brain repo root, with LINKUP_API_KEY in the environment):
    uv run python procurement_research_demo/generate.py
"""

import concurrent.futures
import datetime as dt
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path


API_KEY = os.environ["LINKUP_API_KEY"]
BASE = "https://api.linkup.so/v1"
HERE = Path(__file__).resolve().parent
RAW = HERE / "raw"
RAW.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

COMMON = {
    "mode": "investigate",
    "reasoningDepth": "L",
    "outputType": "sourcedAnswer",
}

# Shared sourcing and citation discipline appended to every query. This is a
# general fix (not per-source patching) for the two defects found in review:
# mismatched citations and reliance on weak blog/marketplace sources.
QUALITY_RULES = """Sourcing and citation rules (apply to every claim):
- For each factual claim, cite the specific source that directly states that claim. Never attach a citation that does not itself contain the claim.
- Never cite a document dated before an event as the source for that event; the citation's own date must be consistent with the claim.
- Strongly prefer primary and authoritative sources: official company filings and press releases, regulators and government bodies, courts, standards bodies, and reputable news or trade press. Do not base a factual claim on SEO/marketing blogs, content farms, or open B2B marketplace listings; use such sources only if nothing better exists, and then explicitly label the claim as low-confidence.
- Label every figure as reported, estimate, or forecast, and give its date or as-of period.
- For any dramatic, contested, or fast-moving event, calibrate your language to what is actually confirmed, distinguish confirmed facts from uncertain ones, note where sources disagree on dates or numbers, and anchor to the most authoritative source available.
- If you cannot find a solid source for a claim, say so explicitly instead of asserting it."""

# Each use case: (order, slug, title, why-it-fits, query).
USE_CASES = [
    (
        1,
        "supplier-discovery",
        "Supplier discovery & shortlisting",
        "Discovery is genuinely web-grounded: suppliers publish their locations, "
        "capabilities, and certifications, so the model can find and compare them.",
        """Find and shortlist commercial suppliers that manufacture wet-glue (cut-and-stack) paper labels for beverage bottles and can serve beverage brands in Europe.

Run several searches across label manufacturers, packaging directories, and trade associations.

For each supplier you find, return in a structured list:
- Company name and website
- Headquarters and main production locations
- Relevant capabilities (wet-glue cut-and-stack, print technologies such as offset/flexo/gravure)
- Quality and sustainability certifications (e.g. ISO 9001, BRCGS Packaging, FSC/PEFC)
- Any publicly stated minimum order quantities or lead times
- One sentence on why they are a credible fit

Prioritise suppliers with verifiable public evidence, and cite the source URL for each supplier. Flag any supplier where the information could not be verified.""",
    ),
    (
        2,
        "supplier-due-diligence",
        "Supplier due diligence & risk",
        "Ownership, financials, litigation, sanctions, recalls and ESG issues are "
        "documented in filings, registries and news — the model's strongest fit.",
        """Produce a supplier due-diligence risk profile on the packaging manufacturer "Crown Holdings, Inc." (the beverage and food can maker).

Cover, using public and reputable sources:
- Corporate identity and ownership: legal entity, HQ, stock listing/ticker, ultimate parent, key subsidiaries
- Financial health signals: latest reported revenue, profitability, debt or credit-rating commentary
- Legal and regulatory exposure: notable litigation, regulatory actions, fines or settlements in the last 5 years
- Sanctions or export-control exposure, if any
- Product recalls or quality issues
- ESG, environmental and labour controversies

Return a short executive risk summary (low/medium/high with reasoning), then the detailed findings by category with dates. Cite the source URL for every material claim and explicitly flag gaps where evidence was not found.""",
    ),
    (
        3,
        "regulatory-compliance",
        "Regulatory & compliance research",
        "Rules, directives and labelling requirements are published by regulators — "
        "verifiable and ideal for multi-source synthesis.",
        """Summarise the regulatory, food-contact, and labelling requirements for importing and selling packaged beer in glass bottles into the European Union market as of 2026.

Cover:
- Mandatory on-label information for beer sold in the EU (e.g. alcoholic strength, net quantity, ingredients/allergens, energy/nutrition, responsible operator, lot/batch)
- Language requirements
- Food-contact material rules relevant to labels, inks and adhesives touching the product
- Packaging and packaging-waste obligations, including deposit-return scheme trends and extended producer responsibility
- Any recent or upcoming changes (e.g. EU Packaging and Packaging Waste Regulation)

Where possible, cite official sources (EUR-Lex, European Commission, national authorities). Note where requirements differ by member state, and flag anything that is still proposed rather than in force.""",
    ),
    (
        4,
        "tariffs-trade-policy",
        "Tariff, trade-policy & sanctions monitoring",
        "Tariff schedules, trade measures and sanctions are officially published and "
        "change over time — high-value, easy to verify.",
        """Research the import tariffs, duties, and trade measures that apply to importing aluminium (unwrought aluminium and aluminium sheet/foil used for packaging) into the United States and into the European Union, as of 2026.

For each of the US and the EU, return:
- Relevant HS/tariff codes for unwrought aluminium and aluminium foil/sheet
- Standard (MFN) duty rates
- Any additional measures: US Section 232 tariffs, anti-dumping or countervailing duties, quotas, and any recent changes in 2025-2026
- Country-specific exemptions or higher rates that materially affect sourcing decisions
- Any relevant sanctions affecting aluminium from specific origins (e.g. Russia)

Cite official sources where possible (USITC HTS, US Customs, EU TARIC, European Commission). Clearly date each figure and flag anything that changed recently or is subject to review.""",
    ),
    (
        5,
        "commodity-price-trends",
        "Commodity & input-cost trend intelligence",
        "Published price indices, trade press and market reports document commodity "
        "direction — good for macro cost planning (not bespoke quotes).",
        """Analyse the price trend of wood pulp (NBSK/BHKP) and packaging/label paper over the last 24 months, and give an outlook for the rest of 2026, for cost planning by a packaging buyer.

Cover:
- The direction and approximate magnitude of price moves over the last 24 months, with figures where available (e.g. USD per tonne, index levels)
- The main drivers (demand, capacity closures, energy and logistics costs, currency)
- Regional differences between Europe, North America and Asia
- The outlook for the remainder of 2026 from reputable analysts or trade press

Cite reputable sources (industry price reports, trade press, producer results). Be explicit about the date of each figure, and distinguish reported facts from forecasts.""",
    ),
    (
        6,
        "disruption-monitoring",
        "Supply-chain disruption monitoring",
        "Disruptions (plant closures, strikes, energy shocks, shortages) are reported "
        "in the news with dates — well suited to time-bounded monitoring.",
        """Identify the most significant supply-chain disruptions and risks over roughly the last 12 months affecting the global glass packaging and glass-bottle supply chain.

Look across news, trade press and company announcements for events such as: energy-cost shocks affecting glass furnaces, plant or furnace closures, strikes, raw-material (soda ash, sand, cullet) shortages, freight and logistics disruruptions, and demand shifts.

For each event, return:
- What happened and when (with the date or period)
- The region and any named companies affected
- The impact on supply, capacity, lead times or price
- A source URL

Order the findings from most to least material, and note any recurring or structural risks the buyer should watch. Do not include events older than about 18 months unless they are still ongoing.""",
    ),
    (
        7,
        "alternative-materials",
        "Alternative-supplier / substitute-material research",
        "Substitute materials and second-sourcing options are widely documented by "
        "vendors and industry press — strong for options analysis.",
        """Research alternative and substitute labelling technologies to wet-glue (cut-and-stack) paper labels for beverage bottles, to help a buyer evaluate options.

Cover at least these alternatives: pressure-sensitive (self-adhesive) labels, linerless labels, shrink-sleeve labels, direct-to-container / direct printing, and washable or recyclable label papers designed for returnable/refillable bottles.

For each alternative, summarise:
- How it works and typical use in the beverage industry
- Relative cost position versus wet-glue (higher/lower and why)
- Application speed and line compatibility
- Recyclability and suitability for returnable/refillable bottles and for recycling wash-off
- Key trade-offs or limitations

Return a comparison overview followed by the per-option detail, and cite source URLs. Where a claim is a general industry view rather than a hard figure, say so.""",
    ),
]


def request_json(method, path, payload=None, retries=4):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    for attempt in range(retries):
        req = urllib.request.Request(
            BASE + path, data=data, headers=HEADERS, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", "replace")
            if err.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep((2**attempt) * 3)
                continue
            raise RuntimeError(f"{method} {path} failed {err.code}: {body[:1000]}")
        except Exception:
            if attempt < retries - 1:
                time.sleep((2**attempt) * 3)
                continue
            raise


def poll(label, task_id):
    interval = 5
    while True:
        task = request_json("GET", f"/research/{task_id}")
        status = task.get("status")
        print(f"[{label}] {task_id} status={status}", flush=True)
        if status in ("completed", "failed"):
            return task
        time.sleep(interval)
        interval = min(15, interval * 1.5)


def runtime(task):
    try:
        created = dt.datetime.fromisoformat(task["createdAt"].replace("Z", "+00:00"))
        updated = dt.datetime.fromisoformat(task["updatedAt"].replace("Z", "+00:00"))
        secs = int((updated - created).total_seconds())
        return f"{secs // 60} min {secs % 60} sec"
    except Exception:
        return "n/a"


def to_markdown(order, title, why, task):
    inp = task.get("input", {})
    out = task.get("output") or {}
    answer = (out.get("answer") or "").strip()
    sources = out.get("sources") or []

    # Show only the use-case question in the report, not the internal shared
    # sourcing/citation rules that are appended to every query at run time.
    query_text = inp.get("q", "")
    marker = "Sourcing and citation rules (apply to every claim):"
    if marker in query_text:
        query_text = query_text.split(marker)[0].strip()

    lines = [
        f"# {order:02d}. {title}",
        "",
        f"**Why this is a good fit for research:** {why}",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| Reasoning depth | {inp.get('reasoningDepth', 'L')} |",
        f"| Mode | {inp.get('mode', 'investigate')} |",
        f"| Output type | {inp.get('outputType', 'sourcedAnswer')} |",
        f"| Status | {task.get('status')} |",
        f"| Run time | {runtime(task)} |",
        f"| Sources | {len(sources)} |",
        "",
        "## Query sent",
        "",
    ]
    for ln in query_text.split("\n"):
        lines.append("> " + ln if ln else ">")
    lines += ["", "## Answer", "", answer or "_No answer returned._", "", "## Sources", ""]

    seen = set()
    n = 0
    for s in sources:
        url = s.get("url", "")
        name = (s.get("name") or url).strip()
        if not url or url in seen:
            continue
        seen.add(url)
        n += 1
        lines.append(f"{n}. [{name}]({url})")
    if n == 0:
        lines.append("_No sources returned._")
    return "\n".join(lines) + "\n"


def main():
    start = time.time()
    print(f"posting {len(USE_CASES)} research jobs at reasoningDepth=L", flush=True)

    posted = []
    for order, slug, title, why, query in USE_CASES:
        full_query = query.strip() + "\n\n" + QUALITY_RULES
        task = request_json("POST", "/research", {"q": full_query, **COMMON})
        print(f"  posted {slug} -> {task['id']}", flush=True)
        posted.append((order, slug, title, why, task["id"]))

    (RAW / "task_ids.json").write_text(
        json.dumps(
            {slug: task_id for _, slug, _, _, task_id in posted}, indent=2
        )
    )

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(posted)) as ex:
        futures = {
            ex.submit(poll, slug, task_id): (order, slug, title, why)
            for order, slug, title, why, task_id in posted
        }
        for fut in concurrent.futures.as_completed(futures):
            order, slug, title, why = futures[fut]
            task = fut.result()
            results[slug] = (order, slug, title, why, task)
            (RAW / f"{order:02d}-{slug}.json").write_text(json.dumps(task, indent=2))
            md = to_markdown(order, title, why, task)
            (HERE / f"{order:02d}-{slug}.md").write_text(md)
            print(f"  saved {order:02d}-{slug}.md ({task.get('status')})", flush=True)

    ordered = sorted(results.values(), key=lambda r: r[0])
    print(
        "DONE "
        + json.dumps(
            {
                "elapsedSec": round(time.time() - start, 1),
                "statuses": {slug: task.get("status") for _, slug, _, _, task in ordered},
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
