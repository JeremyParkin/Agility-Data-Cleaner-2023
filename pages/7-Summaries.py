import json
import re
import warnings
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import streamlit as st
import mig_functions as mig
from openai import OpenAI
from streamlit_tags import st_tags
warnings.filterwarnings("ignore")

st.set_page_config(
    layout="wide",
    page_title="MIG Data Processing App",
    page_icon="https://www.agilitypr.com/wp-content/uploads/2025/01/favicon.png",
)

mig.standard_sidebar()
st.title("Top Stories Summaries")
mig.require_standard_pipeline()


type_dict = {
    "RADIO": "broadcast transcript",
    "TV": "broadcast transcript",
    "PODCAST": "broadcast transcript",
    "ONLINE": "online article",
    "PRINT": "print article",
}

for key, default in {
    "top_story_entity_names": [],
    "top_story_spokespeople": [],
    "top_story_products": [],
    "top_story_guidance": "",
    "top_story_entity_names_seeded": False,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

DEFAULT_MODEL = "gpt-5-mini"
SHORT_SNIPPET_THRESHOLD = 150
DEFAULT_MAX_WORKERS = 8
MAX_RETRIES = 2


def normalize_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize fields needed for this page."""
    df = df.copy()

    defaults = {
        "Headline": "",
        "Example Outlet": "",
        "Example URL": "",
        "Example Type": "",
        "Example Snippet": "",
        "Mentions": 0,
        "Impressions": 0,
        "Chart Callout": "",
        "Top Story Summary": "",
        "Entity Sentiment Label": "",
        "Entity Sentiment Rationale": "",
        "Entity Sentiment": "",
    }

    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default

    text_cols = [
        "Headline",
        "Example Outlet",
        "Example URL",
        "Example Type",
        "Example Snippet",
        "Chart Callout",
        "Top Story Summary",
        "Entity Sentiment Label",
        "Entity Sentiment Rationale",
        "Entity Sentiment",
    ]
    for col in text_cols:
        df[col] = df[col].fillna("").astype(str)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    if "Mentions" in df.columns:
        df["Mentions"] = pd.to_numeric(df["Mentions"], errors="coerce").fillna(0).astype(int)

    if "Impressions" in df.columns:
        df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce").fillna(0).astype(int)

    return df


def escape_markdown(text: str) -> str:
    markdown_special_chars = r"\`*_{}[]()#+-.!$"
    pattern = r"([" + re.escape(markdown_special_chars) + r"])"
    return re.sub(pattern, r"\\\1", str(text))


def build_entity_context(
    primary_name: str,
    alternate_names: List[str],
    spokespeople: List[str],
    products: List[str],
    additional_guidance: str,
) -> str:
    lines = [f"Primary entity: {primary_name.strip()}"]

    if alternate_names:
        lines.append("Alternate names / aliases: " + "; ".join(alternate_names))
    if spokespeople:
        lines.append("Key spokespeople: " + "; ".join(spokespeople))
    if products:
        lines.append("Products / sub-brands / initiatives: " + "; ".join(products))
    if additional_guidance.strip():
        lines.append("Additional user guidance: " + additional_guidance.strip())

    lines.append(
        "Treat references to alternate names, spokespeople, products, sub-brands, and initiatives as relevant to the primary entity only when the coverage clearly maps them back to that entity."
    )

    return "\n".join(lines)


def build_master_prompt(
    row: pd.Series,
    entity_context: str,
) -> str:
    example_type = row.get("Example Type", "")
    snippet = row.get("Example Snippet", "")
    headline = row.get("Headline", "")
    story_type = type_dict.get(example_type, "news story")
    outlet = row.get("Example Outlet", "")

    if example_type in ["RADIO", "TV", "PODCAST"]:
        source_guidance = (
            "The source is a broadcast transcript. Broadcast transcripts may contain unrelated advertisements, tosses, or other segments that should be ignored."
        )
    else:
        source_guidance = f"The source is a {story_type}."

    return f"""
You are a media intelligence analyst producing structured outputs for an executive report.

ENTITY CONTEXT
{entity_context}

OUTPUTS REQUIRED
Return all of the following:
1. chart_callout
2. top_story_summary
3. entity_sentiment_label
4. entity_sentiment_rationale

GLOBAL RULES
- Stay neutral and factual.
- Do not invent facts, implications, motives, or significance.
- Base everything only on the story content provided.
- The brand/entity may appear directly or indirectly through aliases, spokespeople, products, sub-brands, or initiatives.
- If the entity is secondary to the main story, make that clear rather than overstating its importance.
- Avoid vague shorthand like "faces challenges" or "under scrutiny" unless you specify what that means in this story.
- Use present tense where natural.
- No markdown, bullets, or labels inside field values.

FIELD-SPECIFIC RULES

chart_callout:
- One sentence.
- Usually about 12-20 words.
- Suitable for a trend-chart annotation.
- Must clearly say what the story is about and how the entity appears in it.
- Do not sound promotional or analytical.
- Do not mention the outlet unless it materially adds value.

top_story_summary:
- One sentence.
- Usually about 30-50 words.
- Executive-style summary of the story and the entity's role in it.
- Slightly fuller than the chart callout, but still concise.

entity_sentiment_label:
- Must be exactly one of: Positive, Neutral, Negative.

entity_sentiment_rationale:
- One short sentence (typically 8–18 words).
- Explain the sentiment toward the primary entity specifically, not the broader topic.
- Consider references to aliases, spokespeople, products, and sub-brands where relevant.
- Explain WHY the sentiment label applies based on specific coverage dynamics.
- Do NOT repeat or restate the sentiment label (e.g., avoid phrases like "Positive because", "Neutral as", etc.).
- Do NOT define the sentiment category.
- Focus on what the coverage says or does regarding the entity.
- Use concrete signals (e.g., praise, criticism, forecast, legal issue, partnership, expert commentary).

STORY INPUT
Headline: {headline}
Outlet: {outlet}
Type: {example_type or "Unknown"}
{source_guidance}

Body:
{snippet}
""".strip()


def build_prompt_preview(entity_context: str) -> str:
    preview_row = pd.Series({
        "Headline": "Example headline placeholder",
        "Example Outlet": "Example outlet",
        "Example Type": "ONLINE",
        "Example Snippet": "Example story text placeholder showing how the AI will interpret coverage.",
    })

    return build_master_prompt(
        row=preview_row,
        entity_context=entity_context
    )

def get_structured_schema() -> Dict[str, Any]:
    return {
        "name": "top_story_outputs",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "chart_callout": {
                    "type": "string",
                    "description": "Very concise report-ready callout for a trend chart.",
                },
                "top_story_summary": {
                    "type": "string",
                    "description": "Concise executive summary of the story and the entity's role.",
                },
                "entity_sentiment_label": {
                    "type": "string",
                    "enum": ["Positive", "Neutral", "Negative"],
                    "description": "Sentiment toward the primary entity.",
                },
                "entity_sentiment_rationale": {
                    "type": "string",
                    "description": "Short rationale for the sentiment toward the primary entity.",
                },
            },
            "required": [
                "chart_callout",
                "top_story_summary",
                "entity_sentiment_label",
                "entity_sentiment_rationale",
            ],
        },
    }


def extract_response_text(response) -> str:
    """Robustly extract output text from a Responses API response."""
    if hasattr(response, "output_text") and response.output_text:
        return response.output_text

    try:
        parts = []
        for item in response.output:
            if getattr(item, "type", None) != "message":
                continue
            for content in getattr(item, "content", []):
                if getattr(content, "type", None) == "output_text":
                    parts.append(content.text)
        return "\n".join(parts).strip()
    except Exception:
        return ""


def generate_structured_story_outputs(
    client: OpenAI,
    prompt: str,
    model: str = DEFAULT_MODEL,
) -> Dict[str, str]:
    schema = get_structured_schema()

    response = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": "You are a highly skilled media intelligence analyst who produces concise, structured, report-ready outputs.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        text={
            "verbosity": "low",
            "format": {
                "type": "json_schema",
                "name": schema["name"],
                "strict": schema["strict"],
                "schema": schema["schema"],
            },
        },
    )

    raw_text = extract_response_text(response).strip()
    if not raw_text:
        raise ValueError("No structured output text was returned.")

    parsed = json.loads(raw_text)

    sentiment_label = parsed.get("entity_sentiment_label", "").strip()
    sentiment_rationale = parsed.get("entity_sentiment_rationale", "").strip()

    parsed["entity_sentiment_label"] = sentiment_label
    parsed["entity_sentiment_rationale"] = sentiment_rationale
    parsed["entity_sentiment"] = f"{sentiment_label}: {sentiment_rationale}".strip(": ").strip()

    return parsed


def generate_outputs_for_row(
    row_tuple: Tuple[int, Dict[str, Any]],
    entity_context: str,
    api_key: str,
) -> Tuple[int, Dict[str, str], str]:
    """
    Worker function for one story.
    Returns:
      index, parsed_outputs, error_message
    """
    i, row_dict = row_tuple
    row = pd.Series(row_dict)
    snippet = str(row.get("Example Snippet", "") or "")

    if len(snippet) < SHORT_SNIPPET_THRESHOLD:
        return i, {
            "Chart Callout": "Snippet too short to generate callout",
            "Top Story Summary": "Snippet too short to generate summary",
            "Entity Sentiment Label": "Neutral",
            "Entity Sentiment Rationale": "Snippet is too short to assess sentiment reliably.",
            "Entity Sentiment": "Neutral: Snippet is too short to assess sentiment reliably.",
        }, ""

    prompt = build_master_prompt(row, entity_context)

    last_error = ""
    client = OpenAI(api_key=api_key)
    for attempt in range(MAX_RETRIES + 1):
        try:
            parsed = generate_structured_story_outputs(
                client=client,
                prompt=prompt,
                model=DEFAULT_MODEL,
            )
            return i, {
                "Chart Callout": parsed.get("chart_callout", "").strip(),
                "Top Story Summary": parsed.get("top_story_summary", "").strip(),
                "Entity Sentiment Label": parsed.get("entity_sentiment_label", "").strip(),
                "Entity Sentiment Rationale": parsed.get("entity_sentiment_rationale", "").strip(),
                "Entity Sentiment": parsed.get("entity_sentiment", "").strip(),
            }, ""
        except Exception as e:
            last_error = str(e)

    return i, {}, last_error


def build_markdown_output(
    df: pd.DataFrame,
    show_callout: bool,
    show_top_story_summary: bool,
    show_sentiment: bool,
    show_mentions: bool,
    show_impressions: bool,
) -> str:
    markdown_content = ""

    for _, row in df.iterrows():
        head = escape_markdown(row.get("Headline", ""))
        outlet = escape_markdown(row.get("Example Outlet", ""))
        link = escape_markdown(row.get("Example URL", ""))
        date_val = row.get("Date")

        if pd.notna(date_val):
            date_text = pd.to_datetime(date_val).strftime("%B %d, %Y")
        else:
            date_text = ""

        markdown_content += f"__[{head}]({link})__  \n"
        markdown_content += f"_{outlet}_"
        if date_text:
            markdown_content += f" – {date_text}"
        markdown_content += "  \n"



        if show_top_story_summary and "Top Story Summary" in df.columns:
            value = row.get("Top Story Summary", "")
            if pd.notna(value) and str(value).strip():
                markdown_content += f"{value}  \n\n"

        if show_callout and "Chart Callout" in df.columns:
            value = row.get("Chart Callout", "")
            if pd.notna(value) and str(value).strip():
                markdown_content += f"**Chart Callout:** {value}  \n\n"

        if show_sentiment and "Entity Sentiment" in df.columns:
            value = row.get("Entity Sentiment", "")
            if pd.notna(value) and str(value).strip():
                markdown_content += f"_{value}_  \n\n"

        if show_mentions:
            mentions = row.get("Mentions", 0)
            markdown_content += f"**Mentions**: {mentions} &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"

        if show_impressions:
            impressions = row.get("Impressions", 0)
            markdown_content += f"**Impressions**: {int(impressions):,}"

        if show_mentions or show_impressions:
            markdown_content += "<br>"

        markdown_content += "<br>"

    return markdown_content


# Persist advanced inputs across reruns
for key, default in {
    "top_story_alt_names": [],
    "top_story_spokespeople": [],
    "top_story_products": [],
    "top_story_guidance": "",
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


if len(st.session_state["added_df"]) == 0:
    st.error("Please select your TOP STORIES before trying this step.")
else:
    df = normalize_summary_df(st.session_state.added_df.copy())
    df = df.sort_values(by="Date", ascending=True).reset_index(drop=True)

    st.subheader("Generate Analysis")

    summary_col1, summary_col2, summary_col3 = st.columns(3, gap="small")

    with summary_col1:
        client_name = str(st.session_state.get("client_name", "")).strip()

        if not st.session_state.get("top_story_entity_names_seeded", False):
            current_entity_names = st.session_state.get("top_story_entity_names", [])
            current_entity_names = [str(x).strip() for x in current_entity_names if str(x).strip()]

            if client_name and not any(x.lower() == client_name.lower() for x in current_entity_names):
                current_entity_names = [client_name] + current_entity_names

            st.session_state.top_story_entity_names = current_entity_names
            st.session_state.top_story_entity_names_seeded = True

        entity_names = st_tags(
            label="Entity names and aliases",
            text="Primary then aliases",
            maxtags=20,
            value=st.session_state.top_story_entity_names,
            key="top_story_entity_names_tags",
        )
        st.session_state.top_story_entity_names = entity_names

        primary_name = entity_names[0].strip() if entity_names else ""
        alternate_names = [name.strip() for name in entity_names[1:] if name.strip()]


    with summary_col2:
        spokespeople = st_tags(
            label="Key spokespeople",
            # text="Spokespeople",
            maxtags=20,
            value=st.session_state.top_story_spokespeople,
            key="top_story_spokespeople_tags",
        )
        st.session_state.top_story_spokespeople = spokespeople

    with summary_col3:
        products = st_tags(
            label="Products / sub-brands / initiatives",
            text="Press enter to add more",
            maxtags=20,
            value=st.session_state.top_story_products,
            key="top_story_products_tags",
        )
        st.session_state.top_story_products = products



    additional_guidance = st.text_area(
        "**Additional guidance (optional)**",
        value=st.session_state.top_story_guidance,
        height=50,
        help="Optional extra instructions for how the model should interpret or prioritize the entity in coverage.",
        key="top_story_guidance_text",
    )
    st.session_state.top_story_guidance = additional_guidance

    generate_col1, generate_col2 = st.columns([1.5, 3], gap="medium")
    with generate_col1:
        submitted = st.button("Generate All Outputs", type="primary")
    with generate_col2:
        st.caption("Generates chart callout, top story summary, and entity sentiment together for each saved top story.")

    if submitted and not primary_name.strip():
        st.error("Primary entity is required to proceed.")

    if submitted and primary_name.strip():
        entity_context = build_entity_context(
            primary_name=primary_name,
            alternate_names=alternate_names,
            spokespeople=spokespeople,
            products=products,
            additional_guidance=additional_guidance,
        )

        progress_bar = st.progress(0)
        status = st.empty()

        rows_for_workers = [(i, row.to_dict()) for i, row in df.iterrows()]
        completed = 0
        errors = []

        with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
            future_map = {
                executor.submit(
                    generate_outputs_for_row,
                    row_tuple,
                    entity_context,
                    st.secrets["key"],
                ): row_tuple[0]
                for row_tuple in rows_for_workers
            }

            total_items = len(future_map)

            for future in as_completed(future_map):
                i = future_map[future]
                completed += 1

                try:
                    row_index, outputs, error_message = future.result()

                    if error_message:
                        errors.append(f"Story {row_index + 1}: {error_message}")
                    else:
                        for col, value in outputs.items():
                            df.at[row_index, col] = value

                except Exception as e:
                    errors.append(f"Story {i + 1}: {e}")

                progress_bar.progress(int((completed / total_items) * 100))
                status.caption(f"Processed {completed:,} of {total_items:,} stories")

        st.session_state.added_df = df.copy()

        if errors:
            with st.expander(f"Completed with {len(errors)} error(s)", expanded=False):
                for err in errors:
                    st.write(err)

    entity_context = ""

    if primary_name.strip():
        entity_context = build_entity_context(
            primary_name=primary_name,
            alternate_names=alternate_names,
            spokespeople=spokespeople,
            products=products,
            additional_guidance=additional_guidance,
        )
    with st.expander("Show AI prompt preview", expanded=False):
        st.caption("This shows the exact prompt template sent to OpenAI (with example story placeholders).")
        st.code(build_prompt_preview(entity_context), language="text")

    st.divider()
    st.subheader("Copy-Paste Top Stories")
    st.markdown(":mag: **VIEW OPTIONS**")

    show_col1, show_col2, show_col3 = st.columns(3, gap="medium")

    with show_col1:
        show_mentions = st.checkbox("Show mentions", value=False)
        show_impressions = st.checkbox("Show impressions", value=False)

    with show_col2:
        show_top_story_summary = "Top Story Summary" in df.columns and st.checkbox("Show top story summary", value=True)
        show_callout = "Chart Callout" in df.columns and st.checkbox("Show chart callout", value=True)

    with show_col3:
        show_sentiment = "Entity Sentiment" in df.columns and st.checkbox("Show sentiment", value=True)

    if show_mentions or show_impressions:
        st.warning(
            "WARNING: Mentions and Impressions totals reflect exact match headlines on the same date only, not including coverage with headline or date variations."
        )

    st.divider()

    markdown_content = build_markdown_output(
        df=df,
        show_top_story_summary=show_top_story_summary,
        show_callout=show_callout,
        show_sentiment=show_sentiment,
        show_mentions=show_mentions,
        show_impressions=show_impressions,
    )

    st.markdown(markdown_content, unsafe_allow_html=True)