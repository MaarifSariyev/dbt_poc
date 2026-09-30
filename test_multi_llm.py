"""
Çoxlu-model uçdan-uca test:

[Təbii dil sualı] --LLM (Claude/GPT/Gemini)--> [semantic_query JSON]
                  --compiler.py--> [SQL] --DuckDB--> [Nəticə]

Hər provider üçün eyni system prompt, eyni sual dəsti istifadə olunur ki,
hansı modelin daha etibarlı JSON qaytardığını müqayisə edə bilək.
"""
import json
import os
import duckdb
from dotenv import load_dotenv
from compiler import compile_query, load_metrics, load_models

load_dotenv()  # .env faylından açarları oxuyur


def build_system_prompt() -> str:
    metrics = load_metrics()
    models = load_models()

    metrics_desc = "\n".join(
        f"- {name} (əməliyyat: {m['operation']}): {m['description_az']}"
        for name, m in metrics.items()
    )

    all_dims = set()
    categorical_filters = {}
    numeric_filters = set()

    for model in models.values():
        for d in model.get("dimensions", []):
            all_dims.add(d["column"])
            if d.get("allowed_values"):
                categorical_filters[d["column"]] = d["allowed_values"]
        for ms in model.get("measures", []):
            # measure-lar da threshold filter kimi istifadə oluna bilər (interest_rate > 10 kimi)
            numeric_filters.add(ms["column"])

    dims_desc = "\n".join(f"- {d}" for d in all_dims)

    categorical_desc = "\n".join(
        f"- {col} (kateqoriya): icazə verilən dəyərlər {vals}"
        for col, vals in categorical_filters.items()
    )
    numeric_desc = "\n".join(
        f"- {col} (rəqəm, threshold üçün >, <, >=, <= istifadə et)"
        for col in numeric_filters
    )
    filters_desc = categorical_desc + "\n" + numeric_desc

    return f"""Sən bank daxili analitik sualları JSON formatına çevirən köməkçisən.

Mövcud metrikalar (hər birinin əməliyyat tipi göstərilib):
{metrics_desc}

Mövcud qruplaşdırma (group_by) sahələri:
{dims_desc}

Filtr üçün istifadə oluna bilən sahələr və icazə verilən dəyərlər:
{filters_desc}

Filtr üçün istifadə oluna bilən operatorlar: "=", "!=", ">", "<", ">=", "<=", "IN".
Tarix sütunları üçün NİSBİ DÖVR formatı var — operator adi ">=" qalır, DƏYƏR xüsusi mətn olur:
  {{"column": "<tarix_sütunu>", "operator": ">=", "value": "last_n_months:N"}}  (son N ay)
  {{"column": "<tarix_sütunu>", "operator": ">=", "value": "last_n_days:N"}}    (son N gün)
  Məs. "son 30 gün" -> {{"column": "bank_date", "operator": ">=", "value": "last_n_days:30"}}
  Məs. "son 6 ay" -> {{"column": "tarix", "operator": ">=", "value": "last_n_months:6"}}
  Diqqət: dəyər DƏQİQ bu formatda olmalıdır ("last_n_days:30", boşluqsuz, iki nöqtə ilə N
  arasında) — "30 gün", "last_30_days" kimi başqa yazılış QƏBUL EDİLMİR.
  Sabit tarix də ola bilər: {{"column": "...", "operator": ">=", "value": "2025-10-15"}}.

JSON formatını əməliyyat tipinə görə seç:

1. "aggregate" tipli metrikalar üçün:
   {{"metric": "<ad>", "group_by": [...], "filters": [{{"column": "...", "operator": "...", "value": "..."}}]}}
   filters — nəticəyə daxil olacaq sətirləri məhdudlaşdırır (məs. yalnız AZN, yalnız saving).

2. "share" tipli metrikalar üçün İKİ FƏRQLİ filter siyahısı var:
   {{"metric": "<ad>", "group_by": [...], "scope_filters": [...], "subgroup_filters": [...]}}
   - scope_filters: sualın "arasında" dediyi referens çərçivəsi (məs. "saving müştəriləri ARASINDA" -> scope_filters=[account_type=saving]). Bu filtrlər HƏM məxrəcə (100%), HƏM də surətə tətbiq olunur.
   - subgroup_filters: yalnız SURƏTİ (paylananı) əlavə məhdudlaşdıran şərt (məs. "100000-dən çox olanlar" -> subgroup_filters=[balance_amount>100000]).
   - Əgər sual sadəcə "X-in ÜMUMİ portfeldəki payı" formasındadırsa (referens çərçivəsi yoxdur), group_by istifadə et, scope/subgroup_filters boş qalsın.

3. "compare" tipli metrikalar üçün (period_a/period_b əvvəlcədən metrics.yml-də təyin olunub, sən yalnız əlavə filter göndərə bilərsən):
   {{"metric": "<ad>", "filters": [...]}}

4. "breakdown" tipli metrikalar üçün (bir neçə ölçünü — say, məbləğ, çəkili orta, pay — eyni sətirdə birlikdə qaytarır, ölçülərin siyahısı metrics.yml-də əvvəlcədən təyin olunub, sən ona toxunmursan):
   {{"metric": "<ad>", "group_by": [...], "filters": [...]}}
   - group_by ilə nəticəni kateqoriyalara (məs. PRODUCT_CODE, IS_VIP) bölə bilərsən.
   - filters — nəticəyə daxil olacaq sətirləri məhdudlaşdırır (tarix, valyuta və s.).
   - Bu metrikanın QAYTARDIĞI SÜTUNLARI (say, məbləğ, faiz, pay) SEN SEÇMİRSƏN — onlar metrics.yml-də sabitdir, sən yalnız HANSI SƏTİRLƏRİN (filters) və HANSI KATEQORİYALARIN (group_by) daxil olacağını seçirsən.

Yalnız yuxarıda göstərilən icazə verilən dəyərləri filter kimi istifadə et, başqa dəyər uydurma.
Yalnız JSON qaytar, başqa heç nə yazma, markdown code block da yazma.

Əgər sual bu metrikalardan heç biri ilə cavablandırıla bilmirsə, bunu qaytar:
{{"error": "no_matching_metric"}}
"""


def clean_json_text(text: str) -> str:
    """Bəzi modellər ```json ... ``` bloku ilə qaytarır, təmizləyirik."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
    return text.strip()


# ---------------------------------------------------------------------------
# PROVIDER-SPECIFIC ÇAĞIRIŞLAR
# ---------------------------------------------------------------------------

def call_claude(system_prompt: str, question: str) -> str:
    import anthropic
    client = anthropic.Anthropic()
    response = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=200,
        system=system_prompt,
        messages=[{"role": "user", "content": question}],
    )
    return response.content[0].text


def call_openai(system_prompt: str, question: str) -> str:
    from openai import OpenAI
    client = OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o",
        max_tokens=200,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
    )
    return response.choices[0].message.content


def call_gemma(system_prompt: str, question: str) -> str:
    from openai import OpenAI
    client = OpenAI(
        base_url=os.environ.get("GEMMA_SC_BASE_URL"),
        api_key=os.environ.get("GEMMA_SC_API_KEY"),
    )
    response = client.chat.completions.create(
        model="gemma4_think",
        max_tokens=200,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
    )
    return response.choices[0].message.content


def call_azure_gpt(system_prompt: str, question: str) -> str:
    from openai import AzureOpenAI
    client = AzureOpenAI(
        api_key=os.environ.get("AZURE_OPENAI_KEY"),
        azure_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT"),
        api_version=os.environ.get("AZURE_API_VERSION", "2024-12-01-preview"),
    )
    response = client.chat.completions.create(
        model=os.environ.get("AZURE_MODEL", "gpt-4.1"),
        max_tokens=200,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
    )
    return response.choices[0].message.content


PROVIDERS = {
    "claude": call_claude,
    "gpt": call_openai,
    "gemma": call_gemma,
    "azure_gpt": call_azure_gpt,
}


# ---------------------------------------------------------------------------
# ƏSAS TEST MƏNTİQİ
# ---------------------------------------------------------------------------

def run_test(provider_name: str, question: str):
    system_prompt = build_system_prompt()
    call_fn = PROVIDERS[provider_name]

    print(f"\n{'=' * 70}")
    print(f"[{provider_name.upper()}] SUAL: {question}")
    print("=" * 70)

    try:
        raw_output = call_fn(system_prompt, question)
        clean_output = clean_json_text(raw_output)
        semantic_query = json.loads(clean_output)

        # Bəzi modellər tək obyekt əvəzinə [obyekt] formatında list qaytarır — normallaşdırırıq
        if isinstance(semantic_query, list):
            if len(semantic_query) == 0:
                print(f"[XƏTA - Format]  Boş list qaytarıldı")
                return
            if len(semantic_query) > 1:
                print(f"[XƏBƏRDARLIQ]  Model {len(semantic_query)} namizəd qaytardı, birincisi istifadə olunur: {semantic_query}")
            semantic_query = semantic_query[0]

        if not isinstance(semantic_query, dict):
            print(f"[XƏTA - Format]  JSON obyekt gözlənilirdi, gəldi: {type(semantic_query)}")
            return

        print(f"[LLM çıxışı]  {semantic_query}")
    except Exception as e:
        print(f"[XƏTA - LLM çağırışı]  {e}")
        return

    if "error" in semantic_query:
        print("-> Uyğun metrika tapılmadı, fallback lazımdır.")
        return

    try:
        sql = compile_query(semantic_query)
    except Exception as e:
        print(f"[XƏTA - Compiler]  {e}")
        return

    con = duckdb.connect("deposit_real.duckdb")
    result = con.execute(sql).fetchdf()
    con.close()
    print(f"[Nəticə]\n{result}")


if __name__ == "__main__":
    questions = [
        # --- Orijinal 10 sual, real sxem sütun adları ilə ---
        "Saving müştəriləri arasında balansında 100000 manatdan çox pul olan müştərilər ümumi portfelin neçə faizini təşkil edir",
        "USD depozitlərin total portfeldə payı nədir",
        "Hal hazırda manat depozitlərində 10%-in üstü ilə neçə nəfər nə qədər depozit yerləşdirib",
        "USD deposit satışları son 6 ayda əvvəlki 6 aya müqayisədə necə bir trend izləyib",
        "Oktyabr ayının 15-i deposit faizləri 12 və 18 ayliqda aşağı salınmışdır, bu perioddan sonrakı və əvvəlki 12 ayliq AZN depozitlərini müqayisə et",

        # --- Eyni TİPDƏ, amma FƏRQLİ (robustluq testi üçün) ---
        "Deposit müştəriləri arasında balansı 50000 dollardan çox olanlar portfelin neçə faizidir",
        "EUR depozitlərin portfeldəki payı nə qədərdir",
        "VIP müştərilərin portfeldəki payı nə qədərdir",
        "Rəqəmsal (Digital) kanalla açılan depozitlərin son 3 ayda əvvəlki 3 aya nisbətən artımı necədir",

        # --- Hələ dəstəklənməyən suallar (Sistem düzgün imtina edə bilirmi?) ---
        "Neçə saving müştərisi eyni zamanda deposit müştərisidir",
    ]

    # .env-də açarı olan provider-ləri avtomatik seç
    available_providers = []
    if os.environ.get("ANTHROPIC_API_KEY"):
        available_providers.append("claude")
    if os.environ.get("OPENAI_API_KEY"):
        available_providers.append("gpt")
    if os.environ.get("GEMMA_SC_API_KEY"):
        available_providers.append("gemma")
    if os.environ.get("AZURE_OPENAI_KEY"):
        available_providers.append("azure_gpt")

    print(f"Test olunacaq provider-lər: {available_providers}")

    for provider in available_providers:
        for q in questions:
            run_test(provider, q)