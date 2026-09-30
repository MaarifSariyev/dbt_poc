"""
Semantic Layer Compiler v2 — 3 ÜMUMİ ƏMƏLİYYAT üzərində qurulub:

  1. AGGREGATE — bir measure-u (filters + group_by ilə) cəmləmək/saymaq/ortalamaq
  2. SHARE      — bir alt-qrupun bütövə nisbəti (AGGREGATE-in üstündə)
  3. COMPARE    — iki filtr toplusunun (iki dövr, iki kanal, s.) müqayisəsi

Hər metrika (metrics.yml-də) bu 3 əməliyyatdan BİRİNİN parametrləşdirilmiş
çağırışıdır — yeni sual tipi üçün yeni SQL YAZILMIR, mövcud əməliyyat
fərqli parametrlərlə çağırılır.

LLM heç vaxt bu faylı görmür/yazmır — o, yalnız semantic_query JSON-unu doldurur.
"""
import glob
import os
import re

import yaml

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# GREENPLUM UYĞUNLAŞDIRMASI (PoC)
#
# Bu fayl semantic_poc/compiler.py-dən götürülüb. Mühərrik dəyişməyib —
# yalnız aşağıdakı hədəf-spesifik düzəlişlər edilib:
#
#   1. İDENTİFİKATOR DIRNAQLARI — dataiku_ai_coe cədvəllərində sütun adları
#      qarışıq registrlidir ("BANK_DATE", "Branch", "CIF_countd"). PostgreSQL
#      dırnaqsız adları kiçik hərfə salır, ona görə dırnaqsız SQL işləmir.
#   2. round(x, 2) -> round(x::numeric, 2) — PostgreSQL-də double precision
#      üçün iki arqumentli round YOXDUR (işləmə zamanı xəta verir).
#   3. Sətir dəyərləri kaçırılır (' -> '') — SQL injection səthini bağlayır.
#   4. overlap əməliyyatı SİLİNİB — müştəri səviyyəsində açar tələb edir,
#      aqreqasiya olunmuş datamartda belə açar yoxdur.
#   5. Measure sütunlarına filtr QADAĞANDIR — aqreqat sətirdə "balansı
#      100 000-dən çox müştəri" kimi şərt yanlış nəticə verir.
#   6. Non-additive measure-lər (CIF_countd və s.) sətirlər üzrə cəmlənə
#      bilməz — unit açıq şəkildə icazə verməyibsə rədd olunur.
# ---------------------------------------------------------------------------


def _q(identifier: str) -> str:
    """Sütun/cədvəl adını dırnağa alır. Qarışıq registr üçün MƏCBURİDİR."""
    return '"' + str(identifier).replace('"', '""') + '"'


def _lit(value) -> str:
    """Sətir dəyərini kaçıraraq SQL literalına çevirir."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


# Nisbi dövr: modelin "son 12 ay" kimi ifadəni yazacağı YEGANƏ icazəli forma.
# Sərbəst tarix arifmetikası yoxdur — yalnız bu şablon, yalnız ay sayı ilə.
RELATIVE_PERIOD = re.compile(r"^last_n_(months|days):(\d{1,4})$")
MAX_RELATIVE_MONTHS = 120
MAX_RELATIVE_DAYS = 1095


def relative_period_sql(value: str) -> str:
    """
    'last_n_months:12' -> son 12 tam ayın başlanğıcı
    'last_n_days:30'   -> bugündən 30 gün əvvəl

    Yalnız bu iki forma. Sərbəst tarix arifmetikası qəbul edilmir.
    """
    match = RELATIVE_PERIOD.match(str(value))
    if not match:
        raise ValueError(
            f"Yararsız nisbi dövr: {value!r} "
            f"(gözlənilir: last_n_months:N və ya last_n_days:N)"
        )
    unit, amount = match.group(1), int(match.group(2))
    if unit == "months":
        if not 1 <= amount <= MAX_RELATIVE_MONTHS:
            raise ValueError(f"Ay sayı 1-{MAX_RELATIVE_MONTHS} aralığında olmalıdır, gəldi: {amount}")
        return f"(date_trunc('month', current_date) - interval '{amount} months')"
    if not 1 <= amount <= MAX_RELATIVE_DAYS:
        raise ValueError(f"Gün sayı 1-{MAX_RELATIVE_DAYS} aralığında olmalıdır, gəldi: {amount}")
    return f"(current_date - interval '{amount} days')"


# ---------------------------------------------------------------------------
# DÖVR İDARƏETMƏSİ — default pəncərə tətbiqi və cavab üçün AZ təsviri.
#
# Ölçülüb: tarix filtri verilməyəndə satış/axın/uzadılma unit-ləri SƏSSİZCƏ
# bütün tarixi məlumatı (2018-2026) əhatə edirdi. İstifadəçi "son 6 ay"
# gözləyəndə bu, 8 dəfə şişirdilmiş rəqəm verirdi. İndi:
#   1. unit "default_period" elan edibsə və istifadəçi tarix verməyibsə,
#      bu, DETERMİNİST şəkildə tətbiq olunur (apply_default_period);
#   2. hansı dövrün işlədiyi HƏR ZAMAN AZ mətnlə təsvir olunur
#      (describe_period) və cavaba düşür — nə vaxt "son 6 ay", nə vaxt
#      "bütün tarix" olduğu heç vaxt qeyri-müəyyən qalmır.
# ---------------------------------------------------------------------------

def find_time_dimension(model: dict):
    # DİQQƏT: qaytarış tipi qəsdən annotasiya edilmir (str | None Python 3.9-da
    # funksiya tərifi zamanı TypeError verir — eyni səbəbdən PocState-də
    # Optional[] işlədilib). Faktiki qaytarış: str və ya None.
    """Modelin zaman sütunu — hər modeldə ən çox biri var."""
    for dim in model.get("dimensions", []):
        if dim.get("type") == "time":
            return dim["column"]
    return None


def _period_filter_slots(operation: str) -> list:
    """Bu əməliyyat üçün tarix şərtinin ola biləcəyi filtr siyahıları."""
    if operation == "share":
        return ["scope_filters", "subgroup_filters"]
    if operation == "compare":
        return ["period_a_filters", "period_b_filters"]
    return ["filters"]


def accepts_period(unit: dict, model: dict) -> bool:
    """
    Bu unit-ə tarix aralığı tətbiq oluna bilərmi?

    Snapshot modellər (portfolio_snapshot) XEYR — onlarda dövr "ən son gün"dür,
    aralıq deyil; apply_snapshot_default_filter bunu artıq idarə edir.
    'time_series' də XEYR — trend öz təbiətinə görə geniş dövrü əhatə etməlidir.
    """
    if unit.get("operation") == "time_series":
        return False
    if model.get("snapshot_date_column"):
        return False
    return find_time_dimension(model) is not None


def has_explicit_period(semantic_query: dict, unit: dict, model: dict) -> bool:
    """semantic_query-də zaman sütunu üzrə şərt varmı?"""
    time_col = find_time_dimension(model)
    if not time_col:
        return False
    for slot in _period_filter_slots(unit.get("operation", "aggregate")):
        for f in semantic_query.get(slot) or []:
            if isinstance(f, dict) and f.get("column") == time_col:
                return True
    return False


def apply_period(semantic_query: dict, unit: dict, model: dict, period_value: str) -> dict:
    """
    Verilmiş dövrü semantic_query-ə əlavə edir — artıq tarix şərti varsa
    TOXUNMUR (istifadəçinin/agentin açıq dövrü həmişə üstündür).

    Bu, həm unit-in default dövrü, həm də raundlar arası daşınan
    'effective_period' üçün istifadə olunan YEGANƏ mexanizmdir.
    """
    if not period_value or not accepts_period(unit, model):
        return semantic_query
    if has_explicit_period(semantic_query, unit, model):
        return semantic_query

    time_col = find_time_dimension(model)
    operation = unit.get("operation", "aggregate")
    target_slot = "scope_filters" if operation == "share" else "filters"
    result = dict(semantic_query)
    result[target_slot] = list(result.get(target_slot) or []) + [
        {"column": time_col, "operator": ">=", "value": period_value}
    ]
    return result


def apply_default_period(semantic_query: dict, unit: dict, model: dict) -> dict:
    """İstifadəçi dövr verməyibsə, unit-in 'default_period'-unu tətbiq edir."""
    return apply_period(semantic_query, unit, model, unit.get("default_period"))


def period_value_of(semantic_query: dict, unit: dict, model: dict):
    """
    semantic_query-dəki tarix şərtinin DƏYƏRİ (müqayisə üçün normallaşdırılmış).
    Yoxdursa None. Snapshot modellərdə "__snapshot__" qaytarır — bu, aralıq
    deyil, ayrıca bir dövr NÖVÜDÜR və aralıqlarla müqayisə edilməməlidir.
    """
    if model.get("snapshot_date_column"):
        return "__snapshot__"
    time_col = find_time_dimension(model)
    if not time_col:
        return None
    for slot in _period_filter_slots(unit.get("operation", "aggregate")):
        for f in semantic_query.get(slot) or []:
            if isinstance(f, dict) and f.get("column") == time_col:
                return str(f.get("value"))
    return None


def period_kind(semantic_query: dict, unit: dict, model: dict) -> str:
    """
    Dövrün NÖVÜ — uyğunluq yoxlaması üçün.

      snapshot  — "ən son gün" (stock cədvəl); aralıqla müqayisə edilmir
      range     — konkret tarix aralığı
      unbounded — heç bir tarix həddi yoxdur (bütün tarix)

    'trend' ayrıca sayılır: time_series bütün ayları göstərməlidir, ona görə
    həddsiz olması qüsur deyil.
    """
    if model.get("snapshot_date_column"):
        return "snapshot"
    if unit.get("operation") == "time_series":
        return "trend"
    return "range" if period_value_of(semantic_query, unit, model) else "unbounded"


def _describe_single_filter(f: dict) -> str:
    value = f.get("value")
    if isinstance(value, str) and value.startswith("__MAX__:"):
        return "ən son mövcud tarixə əsasən"
    match = RELATIVE_PERIOD.match(str(value)) if isinstance(value, str) else None
    if match:
        unit_word, amount = match.group(1), match.group(2)
        return f"son {amount} {'ay' if unit_word == 'months' else 'gün'}"
    op = f.get("operator", ">=")
    if op in (">=", ">"):
        return f"{value}-dən indiyədək"
    return f"{op} {value}"


def _describe_period_filters(filters: list, time_col: str) -> str:
    matches = [f for f in filters or [] if isinstance(f, dict) and f.get("column") == time_col]
    if not matches:
        return ""
    if len(matches) == 1:
        return _describe_single_filter(matches[0])
    starts = [m for m in matches if m.get("operator") in (">=", ">")]
    ends = [m for m in matches if m.get("operator") in ("<", "<=")]
    if starts and ends:
        # Aralıq bağlıdır — başlanğıcı "...dən indiyədək" kimi açıq
        # oxutmaq YANLIŞDIR (indiyədək deyil, son tarixədək davam edir).
        start_value = starts[0].get("value")
        start_text = (_describe_single_filter(starts[0])
                      if RELATIVE_PERIOD.match(str(start_value)) else str(start_value))
        return f"{start_text} – {ends[0].get('value')}"
    return "; ".join(_describe_single_filter(m) for m in matches)


def describe_period(unit: dict, model: dict, semantic_query: dict) -> str:
    """
    İstifadə olunan tarix aralığının AZ təsviri. Cavabda HƏMİŞƏ göstərilir —
    "son 6 ay" ilə "bütün tarix" arasındakı fərq nəticəni dəyişir və
    istifadəçi bunu görməlidir, modelin bunu yada salmasından asılı olmadan.
    """
    operation = unit.get("operation")
    time_col = find_time_dimension(model)

    if operation == "compare":
        label_a = unit.get("period_a_label", "dövr A")
        label_b = unit.get("period_b_label", "dövr B")
        pa = _describe_period_filters(semantic_query.get("period_a_filters") or [], time_col) or "?"
        pb = _describe_period_filters(semantic_query.get("period_b_filters") or [], time_col) or "?"
        return f"{label_a}: {pa}; {label_b}: {pb}"

    if not time_col:
        return ""

    found = None
    for slot in _period_filter_slots(operation):
        found = _describe_period_filters(semantic_query.get(slot) or [], time_col)
        if found:
            break

    if operation == "time_series":
        current = ("cari ay daxildir" if semantic_query.get("include_current_period")
                  else "cari ay xaric tutulur")
        base = found or "bütün mövcud aylar üzrə"
        return f"{base} ({current})"

    if found:
        return found
    if model.get("snapshot_date_column"):
        return "ən son mövcud tarixə əsasən"
    return "bütün tarixi məlumat üzrə (dövr məhdudiyyəti yoxdur)"


def _safe_label(label: str) -> str:
    """Etiket çıxış sütununun adına düşür — yalnız hərf/rəqəm/alt xətt."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(label or "")):
        raise ValueError(f"Yararsız etiket: {label!r}")
    return str(label)


def _tbl(model: dict) -> str:
    """schema."Cədvəl" — sxem verilibsə onu da əlavə edir."""
    table = model["source_table"]
    if "." in table:
        schema, name = table.split(".", 1)
        return f"{_q(schema)}.{_q(name)}"
    schema = model.get("schema")
    return f"{_q(schema)}.{_q(table)}" if schema else _q(table)


# ---------------------------------------------------------------------------
# YAML YÜKLƏMƏ
# ---------------------------------------------------------------------------

def load_models() -> dict:
    models = {}
    for path in glob.glob(os.path.join(BASE_DIR, "models", "*.yml")):
        with open(path, encoding="utf-8") as f:
            m = yaml.safe_load(f)
            models[m["name"]] = m
    return models


def load_metrics() -> dict:
    with open(os.path.join(BASE_DIR, "units.yml"), encoding="utf-8") as f:
        metrics_list = yaml.safe_load(f)
    return {m["name"]: m for m in metrics_list}


def load_relationships() -> list:
    with open(os.path.join(BASE_DIR, "relationships.yml"), encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# VALIDATION — hər filter/group_by sxemə uyğunluğa görə yoxlanılır
# ---------------------------------------------------------------------------

def _get_dims_and_measures(model: dict):
    dims = {d["column"]: d for d in model.get("dimensions", [])}
    measures = {m["column"]: m for m in model.get("measures", [])}
    return dims, measures


def _is_numeric(value) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    try:
        float(str(value))
        return True
    except (TypeError, ValueError):
        return False


def _is_date_like(value) -> bool:
    text = str(value)
    if RELATIVE_PERIOD.match(text):
        return True
    return bool(re.match(r"^\d{4}-\d{2}(-\d{2})?", text))


def validate_filter(filter_item: dict, model: dict) -> tuple:
    column = filter_item["column"]
    value = filter_item["value"]
    operator = filter_item.get("operator", "=")

    # Operator böyük/kiçik hərfə həssas olmasın (LLM "in" yoxsa "IN" göndərə bilər)
    operator = operator.upper() if operator.lower() == "in" else operator

    if operator not in ("=", "!=", ">", "<", ">=", "<=", "IN"):
        raise ValueError(f"İcazə verilməyən operator: {operator}")

    dims, measures = _get_dims_and_measures(model)

    # Case-insensitive uyğunlaşdırma: LLM sütun adını fərqli böyük/kiçik
    # hərflə yazsa (interest_rate vs INTEREST_RATE), real adı tapırıq
    if column not in dims and column not in measures:
        column_lower_map = {c.lower(): c for c in list(dims.keys()) + list(measures.keys())}
        real_column = column_lower_map.get(column.lower())
        if real_column is None:
            raise ValueError(
                f"Filter sütunu '{column}' modeldə ('{model['name']}') tapılmadı"
            )
        column = real_column  # real adla əvəz et

    # PoC QAYDASI: measure sütunlarına filtr qoymaq olmaz. Aqreqat cədvəldə
    # measure artıq cəmlənmiş dəyərdir — "balansı 100 000-dən çox olan
    # müştərilər" kimi şərt fərdi müştəriyə deyil, cəmə tətbiq olunardı və
    # səssizcə yanlış cavab verərdi.
    if column in measures:
        raise ValueError(
            f"'{column}' bir measure-dur; aqreqasiya olunmuş datamartda measure "
            f"üzrə filtr fərdi müştəri/müqavilə şərtini ifadə edə bilməz"
        )

    # generate_models.py TAM NULL sütunu "unusable" işarələyir. Belə sütun
    # üzrə filtr həmişə boş nəticə verər — səssiz sıfır əvəzinə açıq xəta.
    if column in dims and dims[column].get("unusable"):
        raise ValueError(
            f"'{column}' sütunu hazırda datamartda tam boşdur (NULL) — "
            f"filtr və ya qruplaşdırma üçün istifadə edilə bilməz"
        )

    # TİP YOXLAMASI. Enum siyahısı olmayan sütunlarda (məs. 39 fərqli 'term')
    # əvvəllər heç bir dəyər yoxlanışı yox idi: model "term = 'Son 12 ay'"
    # göndərdi, sətir bigint sütuna düşdü və sorğu İCRA VAXTI qırıldı.
    # İndi bu, kompilyasiya mərhələsində, aydın mesajla dayandırılır.
    if column in dims:
        data_type = dims[column].get("data_type")
        for candidate in (value if isinstance(value, list) else [value]):
            if data_type == "number" and not _is_numeric(candidate):
                raise ValueError(
                    f"'{column}' rəqəm sütunudur, '{candidate}' isə mətndir. "
                    f"Tarix aralığı üçün rəqəm sütununu istifadə etmək olmaz — "
                    f"zaman sütununda 'last_n_months:N' yazın"
                )
            if data_type == "date" and not _is_date_like(candidate):
                raise ValueError(
                    f"'{column}' tarix sütunudur, '{candidate}' uyğun deyil. "
                    f"Qəbul olunur: YYYY-MM-DD və ya 'last_n_months:N'"
                )

    if column in dims:
        allowed = dims[column].get("allowed_values")
        if allowed is not None:
            values_to_check = value if isinstance(value, list) else [value]
            for v in values_to_check:
                if v not in allowed:
                    raise ValueError(
                        f"'{v}' dəyəri '{column}' üçün icazə verilən siyahıda deyil: {allowed}"
                    )

    return column, operator, value


# ---------------------------------------------------------------------------
# JOIN HƏLLİ — relationships.yml-ə əsasən, model başqa modeldəki sütuna
# istinad edəndə lazımi JOIN-i tapır
# ---------------------------------------------------------------------------

def resolve_column(col_name: str, model: dict, models: dict, relationships: list, alias: str = "m"):
    """
    'branch_name' kimi bir sütun ver — öz modelindədirsə birbaşa qaytarır,
    başqa modeldədirsə relationships.yml-dən JOIN tapıb qaytarır.
    Qaytarır: (sql_ifadə, join_sql yoxsa None)

    Case-insensitive: LLM sütun adını fərqli böyük/kiçik hərflə yazsa
    (is_vip vs IS_VIP), real adı tapır.
    """
    dims, measures = _get_dims_and_measures(model)

    if col_name not in dims and col_name not in measures:
        column_lower_map = {c.lower(): c for c in list(dims.keys()) + list(measures.keys())}
        real_name = column_lower_map.get(col_name.lower())
        if real_name is not None:
            col_name = real_name

    if col_name in dims and dims[col_name].get("unusable"):
        raise ValueError(
            f"'{col_name}' sütunu hazırda datamartda tam boşdur (NULL) — "
            f"filtr və ya qruplaşdırma üçün istifadə edilə bilməz"
        )

    if col_name in dims or col_name in measures:
        expr = f"{alias}.{_q(col_name)}"
        # IS_VIP kimi sütunlarda NULL real biznes mənası daşıyır ("VIP deyil").
        # Qruplaşdırmada NULL ayrıca səbət yaratmasın deyə modeldə elan olunan
        # etiketə çevrilir — ekspertin SQL-lərindəki coalesce(...) ilə eyni.
        if col_name in dims and dims[col_name].get("null_as") is not None:
            expr = f"coalesce({expr}, {_lit(dims[col_name]['null_as'])})"
        return expr, None

    for rel in relationships:
        if rel["from_model"] == model["name"]:
            target_model = models[rel["to_model"]]
            target_dims, target_measures = _get_dims_and_measures(target_model)

            target_col_name = col_name
            if target_col_name not in target_dims and target_col_name not in target_measures:
                target_lower_map = {c.lower(): c for c in list(target_dims.keys()) + list(target_measures.keys())}
                real_target_name = target_lower_map.get(target_col_name.lower())
                if real_target_name is not None:
                    target_col_name = real_target_name

            if target_col_name in target_dims or target_col_name in target_measures:
                join_alias = rel["to_model"][0]
                join_sql = (
                    f"{rel['join_type']} join {_tbl(target_model)} {join_alias} "
                    f"on {alias}.{_q(rel['from_column'])} = {join_alias}.{_q(rel['to_column'])}"
                )
                return f"{join_alias}.{_q(target_col_name)}", join_sql

    raise ValueError(f"Sütun '{col_name}' heç bir modeldə tapılmadı (model: {model['name']})")


def build_where_sql(filters: list, model: dict, models: dict, relationships: list, alias: str = "m") -> tuple:
    """
    Filtrləri doğrulayır və WHERE bəndinə çevirir.
    Qaytarır: (şərtlər_siyahısı, əlavə_join_sql_seti)

    Xüsusi qeyd: value "__MAX__:table.column" formatındadırsa, bu, sabit
    dəyər deyil — "= (SELECT MAX(column) FROM table)" alt-sorğusuna çevrilir
    (apply_snapshot_default_filter tərəfindən yaradılır, validate_filter-dən keçmir).
    """
    conditions = []
    joins = set()
    for f in filters or []:
        if isinstance(f.get("value"), str) and f["value"].startswith("__MAX__:"):
            _, ref = f["value"].split(":", 1)
            table, column = ref.rsplit(".", 1)
            table_sql = ".".join(_q(part) for part in table.split("."))
            conditions.append(
                f"{alias}.{_q(f['column'])} = (select max({_q(column)}) from {table_sql})"
            )
            continue

        column, operator, value = validate_filter(f, model)
        col_sql, join_sql = resolve_column(column, model, models, relationships, alias)
        if join_sql:
            joins.add(join_sql)

        if operator == "IN":
            if not isinstance(value, list):
                raise ValueError(f"'IN' operatoru üçün 'value' siyahı (list) olmalıdır, gəldi: {type(value)}")
            formatted_values = ", ".join(_lit(v) for v in value)
            conditions.append(f"{col_sql} in ({formatted_values})")
        elif isinstance(value, str) and RELATIVE_PERIOD.match(value):
            conditions.append(f"{col_sql} {operator} {relative_period_sql(value)}")
        else:
            conditions.append(f"{col_sql} {operator} {_lit(value)}")
    return conditions, joins


def row_count_expr(model: dict, alias: str = "m") -> str:
    """
    "Neçə ədəd?" sualının DOĞRU ifadəsi.

    Aqreqasiya olunmuş cədvəldə bir sətir bir müqavilə DEYİL — ölçü
    kombinasiyasıdır. Ona görə count(*) sətirləri sayır, müqavilələri yox.
    Model 'row_count_measure' elan edibsə, həqiqi say onun cəmidir.

    Ölçülüb (satış cədvəli): count(*) = 25,057 sətir, amma
    sum("CONTRACT_REF_NO_count") = 407,052 müqavilə — 16 dəfə fərq.
    """
    measure = model.get("row_count_measure")
    return f"sum({alias}.{_q(measure)})" if measure else "count(*)"


def row_count_case_expr(model: dict, condition: str, alias: str = "m") -> str:
    """Şərtə uyğun sətirlərin say ifadəsi (yuxarıdakı ilə eyni məntiq)."""
    measure = model.get("row_count_measure")
    if measure:
        return f"sum(case when {condition} then {alias}.{_q(measure)} else 0 end)"
    return f"count(case when {condition} then 1 end)"


def get_default_agg(model: dict, measure_column: str, allow_approximate: bool = False) -> str:
    for ms in model["measures"]:
        if ms["column"] == measure_column:
            # Non-additive measure (məs. CIF_countd) sətirlər üzrə cəmlənəndə
            # eyni müştəri bir neçə məhsulda təkrar sayılır. Ölçülüb: bir gündə
            # 12.6% şişmə. Unit açıq şəkildə icazə verməyibsə, rədd edirik.
            if ms.get("additive") is False and not allow_approximate:
                raise ValueError(
                    f"'{measure_column}' additive deyil — sətirlər üzrə cəmlənməsi "
                    f"təkrar saymaya gətirir. Unit-də 'allow_approximate: true' "
                    f"qeyd olunmayıbsa bu ölçü istifadə edilə bilməz"
                )
            return ms["default_agg"]
    raise ValueError(f"Measure '{measure_column}' modeldə tapılmadı")


def apply_snapshot_default_filter(filters: list, model: dict, temporal_scope: str = "latest_snapshot") -> list:
    """
    Əgər model 'snapshot_date_column' işarəsinə malikdirsə (yəni hər sətir
    müstəqil bir günün tam mənzərəsidir, günlər üzrə cəmlənə bilməz) və
    filtrlərdə bu sütun üçün HEÇ BİR şərt yoxdursa, avtomatik olaraq
    "ən son tarix" filtrini əlavə edir.

    Bu, LLM/istifadəçi tarix filtrini unutsa belə, sistemin bütün tarixləri
    cəmləyib mənasız, şişirdilmiş nəticə verməsinin qarşısını alır.

    temporal_scope parametri metrics.yml-dən gəlir:
      - "latest_snapshot" (default): yuxarıdakı avtomatik filtr tətbiq olunur
      - "all_time": avtomatik filtr TƏTBİQ OLUNMUR — bütün tarixi məlumat
        üzərində işləyir. Bu, "hal hazırda aktiv olan bütün müqavilələr"
        kimi sualların, konkret bir günün "boş" ola biləcəyi riskini keçir.
    """
    if temporal_scope == "all_time":
        return filters

    snapshot_col = model.get("snapshot_date_column")
    if not snapshot_col:
        return filters  # bu model snapshot tipli deyil, dəyişiklik lazım deyil

    already_filtered = any(f["column"] == snapshot_col for f in filters)
    if already_filtered:
        return filters  # istifadəçi/LLM artıq tarix veribdir, toxunmuruq

    # Avtomatik: "= (SELECT MAX(snapshot_col) FROM source_table)" filtri əlavə et
    schema = model.get("schema")
    qualified = f"{schema}.{model['source_table']}" if schema else model["source_table"]
    return filters + [{
        "column": snapshot_col,
        "operator": "=",
        "value": f"__MAX__:{qualified}.{snapshot_col}",
    }]


# ---------------------------------------------------------------------------
# ƏMƏLİYYAT 1 — AGGREGATE
# Bir measure-u (filters + group_by ilə) cəmləyir/sayır/ortalayır.
# ---------------------------------------------------------------------------

AGGREGATE_TEMPLATE = """select
    {group_by_select}{select_measures}
from {source_table} m
{joins}
{where_clause}
{group_by_clause}
{order_by_clause}"""


def op_aggregate(model: dict, models: dict, relationships: list,
                  measure_column: str, filters: list, group_by: list,
                  metric_name: str, count_distinct_column: str = None,
                  temporal_scope: str = "latest_snapshot") -> str:
    agg_func = get_default_agg(model, measure_column)

    filters = apply_snapshot_default_filter(filters, model, temporal_scope)
    conditions, joins = build_where_sql(filters, model, models, relationships)

    group_by_parts = []
    for col in group_by or []:
        col_sql, join_sql = resolve_column(col, model, models, relationships)
        group_by_parts.append(col_sql)
        if join_sql:
            joins.add(join_sql)

    # count_distinct veriləndə, HƏM say, HƏM cəm sütun kimi qaytarılır
    if count_distinct_column:
        select_measures = (
            f"count(distinct m.{_q(count_distinct_column)}) as customer_count,\n    "
            f"{agg_func}(m.{_q(measure_column)}) as total_amount"
        )
    else:
        select_measures = f"{agg_func}(m.{_q(measure_column)}) as {_q(metric_name)}"

    group_by_select = (", ".join(group_by_parts) + ",\n    ") if group_by_parts else ""
    where_clause = ("where " + " and ".join(conditions)) if conditions else ""
    group_by_clause = f"group by {', '.join(group_by_parts)}" if group_by_parts else ""
    order_by_clause = f"order by {', '.join(group_by_parts)}" if group_by_parts else ""

    return AGGREGATE_TEMPLATE.format(
        group_by_select=group_by_select,
        select_measures=select_measures,
        source_table=_tbl(model),
        joins="\n".join(joins),
        where_clause=where_clause,
        group_by_clause=group_by_clause,
        order_by_clause=order_by_clause,
    )


# ---------------------------------------------------------------------------
# ƏMƏLİYYAT 2 — SHARE
# Bir alt-qrupun (filters ilə təyin olunan) bütövə nisbəti.
# ---------------------------------------------------------------------------

SHARE_TEMPLATE = """with subgroup as (
    select
        {group_by_select}{agg_func}(m.{measure_column}) as subgroup_value
    from {source_table} m
    {joins}
    {subgroup_where}
    {group_by_clause}
),
total as (
    select {agg_func}(m.{measure_column}) as total_value
    from {source_table} m
    {joins}
    {total_where}
)
select
    {group_by_select_out}subgroup_value,
    total.total_value,
    round((100.0 * subgroup_value / total.total_value)::numeric, 2) as {metric_name}
from subgroup, total
{order_by_clause}"""


def op_share(model: dict, models: dict, relationships: list,
             measure_column: str, subgroup_filters: list, total_filters: list,
             group_by: list, metric_name: str, temporal_scope: str = "latest_snapshot") -> str:
    agg_func = get_default_agg(model, measure_column)

    subgroup_filters = apply_snapshot_default_filter(subgroup_filters, model, temporal_scope)
    total_filters = apply_snapshot_default_filter(total_filters, model, temporal_scope)

    subgroup_conditions, joins1 = build_where_sql(subgroup_filters, model, models, relationships)
    total_conditions, joins2 = build_where_sql(total_filters, model, models, relationships)
    joins = joins1 | joins2

    group_by_parts = []
    for col in group_by or []:
        col_sql, join_sql = resolve_column(col, model, models, relationships)
        group_by_parts.append(col_sql)
        if join_sql:
            joins.add(join_sql)

    group_by_select = (", ".join(group_by_parts) + ",\n        ") if group_by_parts else ""
    group_by_select_out = (
        ", ".join(f"subgroup.{p.split('.')[-1]}" for p in group_by_parts) + ",\n    "
    ) if group_by_parts else ""
    group_by_clause = f"group by {', '.join(group_by_parts)}" if group_by_parts else ""
    order_by_clause = (
        f"order by {', '.join(f'subgroup.{p.split(chr(46))[-1]}' for p in group_by_parts)}"
        if group_by_parts else f"order by {_q(metric_name)} desc"
    )

    subgroup_where = ("where " + " and ".join(subgroup_conditions)) if subgroup_conditions else ""
    total_where = ("where " + " and ".join(total_conditions)) if total_conditions else ""

    return SHARE_TEMPLATE.format(
        group_by_select=group_by_select,
        group_by_select_out=group_by_select_out,
        agg_func=agg_func,
        measure_column=_q(measure_column),
        source_table=_tbl(model),
        joins="\n".join(joins),
        subgroup_where=subgroup_where,
        total_where=total_where,
        group_by_clause=group_by_clause,
        order_by_clause=order_by_clause,
        metric_name=_q(metric_name),
    )


# ---------------------------------------------------------------------------
# ƏMƏLİYYAT 2b — SHARE (CASE WHEN formatı)
# Mövcud "share"-in alternativ çıxış forması: iki kateqoriyanı (məs.
# rəqəmsal/ənənəvi) EYNİ SƏTİRDƏ, yan-yana sütun kimi göstərir —
# subquery/CTE-based bölünmə əvəzinə. Real Oracle SQL-lərdə tez-tez
# rast gəlinən "hər term üçün bir sətir, kateqoriyalar sütun kimi" formatı.
# ---------------------------------------------------------------------------

SHARE_CASE_WHEN_TEMPLATE = """select
    {group_by_select}
    sum(case when m.{case_column} = {case_value_a} then m.{measure_column} else 0 end) as {label_a}_amount,
    sum(case when m.{case_column} = {case_value_b} then m.{measure_column} else 0 end) as {label_b}_amount,
    sum(m.{measure_column}) as total_amount,
    round(
        (100.0 * sum(case when m.{case_column} = {case_value_a} then m.{measure_column} else 0 end)
        / nullif(sum(m.{measure_column}), 0))::numeric,
        2
    ) as {label_a}_amount_share_pct,
    {count_a_expr} as {label_a}_count,
    {count_b_expr} as {label_b}_count,
    {count_total_expr} as total_count,
    round(
        (100.0 * {count_a_expr}
        / nullif({count_total_expr}, 0))::numeric,
        2
    ) as {label_a}_count_share_pct
from {source_table} m
{joins}
{where_clause}
{group_by_clause}
{order_by_clause}"""


def op_share_case_when(model: dict, models: dict, relationships: list,
                        measure_column: str, case_column: str,
                        case_value_a: str, case_value_b: str,
                        label_a: str, label_b: str,
                        filters: list, group_by: list, metric_name: str,
                        temporal_scope: str = "latest_snapshot") -> str:
    filters = apply_snapshot_default_filter(filters, model, temporal_scope)
    conditions, joins = build_where_sql(filters, model, models, relationships)
    where_clause = ("where " + " and ".join(conditions)) if conditions else ""

    group_by_parts = []
    for col in group_by or []:
        col_sql, join_sql = resolve_column(col, model, models, relationships)
        group_by_parts.append(col_sql)
        if join_sql:
            joins.add(join_sql)

    group_by_select = (", ".join(group_by_parts) + ",") if group_by_parts else ""
    group_by_clause = f"group by {', '.join(group_by_parts)}" if group_by_parts else ""
    order_by_clause = f"order by {', '.join(group_by_parts)}" if group_by_parts else ""

    cond_a = f"m.{_q(case_column)} = {_lit(case_value_a)}"
    cond_b = f"m.{_q(case_column)} = {_lit(case_value_b)}"
    return SHARE_CASE_WHEN_TEMPLATE.format(
        count_a_expr=row_count_case_expr(model, cond_a),
        count_b_expr=row_count_case_expr(model, cond_b),
        count_total_expr=row_count_expr(model),
        group_by_select=group_by_select,
        case_column=_q(case_column),
        case_value_a=_lit(case_value_a),
        case_value_b=_lit(case_value_b),
        label_a=_safe_label(label_a),
        label_b=_safe_label(label_b),
        measure_column=_q(measure_column),
        source_table=_tbl(model),
        joins="\n".join(joins),
        where_clause=where_clause,
        group_by_clause=group_by_clause,
        order_by_clause=order_by_clause,
    )


# ---------------------------------------------------------------------------
# ƏMƏLİYYAT 3 — COMPARE
# İki "dövr"ün (və ya iki filtr toplusunun) müqayisəsi.
# ---------------------------------------------------------------------------

COMPARE_TEMPLATE = """select
    '{period_a_label}' as period_label,
    {group_by_select}
    {agg_func}(m.{measure_column}) as period_value,
    count(*) as row_count
from {source_table} m
{joins}
{where_a}
{group_by_clause}

union all

select
    '{period_b_label}' as period_label,
    {group_by_select}
    {agg_func}(m.{measure_column}) as period_value,
    count(*) as row_count
from {source_table} m
{joins}
{where_b}
{group_by_clause}

order by period_label{order_by_extra}"""


def op_compare(model: dict, models: dict, relationships: list,
               measure_column: str, period_a_filters: list, period_b_filters: list,
               period_a_label: str, period_b_label: str, metric_name: str,
               group_by: list = None) -> str:
    agg_func = get_default_agg(model, measure_column)

    conditions_a, joins1 = build_where_sql(period_a_filters, model, models, relationships)
    conditions_b, joins2 = build_where_sql(period_b_filters, model, models, relationships)
    joins = joins1 | joins2

    where_a = ("where " + " and ".join(conditions_a)) if conditions_a else ""
    where_b = ("where " + " and ".join(conditions_b)) if conditions_b else ""

    group_by_parts = []
    for col in group_by or []:
        col_sql, join_sql = resolve_column(col, model, models, relationships)
        group_by_parts.append(col_sql)
        if join_sql:
            joins.add(join_sql)

    if group_by_parts:
        # group_by sütunları həm SELECT-də (period_label-dən sonra), həm GROUP BY-da olmalıdır
        group_by_select = ", ".join(group_by_parts) + ","
        group_by_clause = "group by " + ", ".join(group_by_parts)
        # DÜZƏLİŞ: ORDER BY xarici UNION ALL sorğusuna aiddir — orada "m"
        # aliası görünmür. Ona görə sıralama çıxış sütun adına görə edilir,
        # "m.sütun" ifadəsinə görə yox. (Orijinal mühərrikdə bu xəta var idi;
        # DuckDB bağışlayır, Greenplum yox.)
        order_by_extra = ", " + ", ".join(part.split(".", 1)[-1] for part in group_by_parts)
    else:
        group_by_select = ""
        group_by_clause = ""
        order_by_extra = ""

    return COMPARE_TEMPLATE.format(
        agg_func=agg_func,
        measure_column=_q(measure_column),
        source_table=_tbl(model),
        joins="\n".join(joins),
        where_a=where_a,
        where_b=where_b,
        period_a_label=period_a_label,
        period_b_label=period_b_label,
        group_by_select=group_by_select,
        group_by_clause=group_by_clause,
        order_by_extra=order_by_extra,
    )


# ---------------------------------------------------------------------------
# ƏMƏLİYYAT 4 — TIME_SERIES
# Hər dövr (ay/gün) üçün ayrıca sətir, əvvəlki dövrə nisbətən faiz dəyişimi
# (LAG OVER). "Toplu iki blok müqayisəsi" (COMPARE) əvəzinə, "xətt boyu trend"
# lazım olan suallar üçün.
# ---------------------------------------------------------------------------

TIME_SERIES_TEMPLATE = """with periods as (
    select
        date_trunc('{granularity}', m.{time_column}) as period,
        {group_by_select}{agg_func}(m.{measure_column}) as period_value,
        {row_count_expr} as period_row_count
    from {source_table} m
    {joins}
    {where_clause}
    group by date_trunc('{granularity}', m.{time_column}){group_by_extra}
)
select
    period,
    {group_by_select_out}period_value,
    period_row_count,
    lag(period_value) over ({partition_clause}order by period) as prior_period_value,
    round(
        (100.0 * (period_value - lag(period_value) over ({partition_clause}order by period))
        / nullif(lag(period_value) over ({partition_clause}order by period), 0))::numeric,
        2
    ) as {metric_name}
from periods
order by period desc{order_extra}"""


def current_period_guard(time_column_sql: str, granularity: str) -> str:
    """
    Yarımçıq cari dövrü kənarlaşdıran şərt.

    NİYƏ DETERMİNİSTDİR: aylıq trenddə cari ay hələ bitməyib — ayın 3-ü
    sorğu versən, həmin ay 3 günlük satışı göstərir və əvvəlki tam aya
    nisbətən -97% kimi SAXTA eniş yaradır (ölçülüb: sentyabr 6.9 mln,
    avqust 274.6 mln). Bu, məlumat problemi deyil, müqayisə səhvidir, ona
    görə şablonun özündə bağlanır — modelin filtr yazmasından asılı deyil.

    İstifadəçi AÇIQ şəkildə "bu ay indiyə qədər" istəyirsə, unit
    'include_current_period' ilə çağırılır və bu şərt tətbiq olunmur.
    """
    return f"{time_column_sql} < date_trunc('{granularity}', current_date)"


def op_time_series(model: dict, models: dict, relationships: list,
                    measure_column: str, filters: list, time_column: str,
                    granularity: str, metric_name: str,
                    include_current_period: bool = False,
                    group_by: list = None) -> str:
    agg_func = get_default_agg(model, measure_column)

    filters = apply_snapshot_default_filter(filters, model)
    conditions, joins = build_where_sql(filters, model, models, relationships)

    # Yarımçıq cari dövr DEFAULT olaraq kənarlaşdırılır — şablonun özündə,
    # modelin filtrindən asılı olmayaraq. Yalnız istifadəçi açıq şəkildə
    # "bu ay indiyə qədər" deyəndə daxil edilir.
    if not include_current_period:
        conditions = conditions + [
            current_period_guard(f"m.{_q(time_column)}", granularity)
        ]

    where_clause = ("where " + " and ".join(conditions)) if conditions else ""

    # Qruplaşdırma: aylıq trend + məhsul/term kimi ölçü (Q6, Q13, Q19).
    # LAG hər qrup daxilində ayrıca hesablanmalıdır, əks halda faiz dəyişməsi
    # qruplar arasında sürüşür və mənasız olur.
    group_by_parts = []
    for column in group_by or []:
        column_sql, join_sql = resolve_column(column, model, models, relationships)
        group_by_parts.append(column_sql)
        if join_sql:
            joins.add(join_sql)

    if group_by_parts:
        bare = [_q(column) for column in (group_by or [])]
        labelled = [f"{expr} as {name}" if expr != f"m.{name}" else expr
                    for expr, name in zip(group_by_parts, bare)]
        group_by_select = ", ".join(labelled) + ",\n        "
        group_by_select_out = ", ".join(bare) + ",\n    "
        group_by_extra = ", " + ", ".join(group_by_parts)
        partition_clause = "partition by " + ", ".join(bare) + " "
        order_extra = ", " + ", ".join(bare)
    else:
        group_by_select = group_by_select_out = group_by_extra = partition_clause = order_extra = ""

    return TIME_SERIES_TEMPLATE.format(
        group_by_select=group_by_select,
        group_by_select_out=group_by_select_out,
        group_by_extra=group_by_extra,
        partition_clause=partition_clause,
        order_extra=order_extra,
        row_count_expr=row_count_expr(model),
        granularity=granularity,
        agg_func=agg_func,
        measure_column=_q(measure_column),
        time_column=_q(time_column),
        source_table=_tbl(model),
        joins="\n".join(joins),
        where_clause=where_clause,
        metric_name=_q(metric_name),
    )


# ---------------------------------------------------------------------------
# OVERLAP əməliyyatı bu PoC-də YOXDUR.
#
# "Neçə saving müştərisi eyni zamanda deposit müştərisidir" sualı iki müştəri
# çoxluğunun kəsişməsini tələb edir — bunun üçün unikal müştəri açarı (cif)
# lazımdır. dataiku_ai_coe aqreqat cədvəllərində belə açar YOXDUR: CIF_countd
# və CIF_distinct sütunları saydır, identifikator deyil. Onları kəsişdirmək
# riyazi olaraq mümkün deyil, təxmin etmək isə səssizcə yanlış cavab verərdi.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# ƏMƏLİYYAT 6 — BREAKDOWN
#
# Ekspertin sual dəstinin böyük hissəsi eyni formadadır: ölçülər üzrə
# qruplaşdır və BİR NEÇƏ fərqli tipli göstərici qaytar — cəm, çəkili orta,
# ümumidəki pay, iki cəmin nisbəti, şərtli cəm.
#
# Bu göstəricilərin siyahısı UNIT-də İNSAN TƏRƏFİNDƏN yazılır (outputs).
# Model yalnız filtr və qruplaşdırma təklif edir — çıxış formasını dəyişə
# bilmir. Beləliklə yeni sual tipi üçün YAML yazılır, SQL yox.
#
# Dəstəklənən kind-lər:
#   sum             — sadə cəm
#   weighted_avg    — sum(ölçü * çəki) / sum(çəki)   (sadə AVG YANLIŞDIR)
#   share_of_total  — 100 * sum(x) / sum(sum(x)) over (partition by ...)
#   ratio           — sum(a) / sum(b)  (məs. orta bilet, müştəri başına)
#   sum_where       — yalnız icazə verilən dəyərlər üçün cəm (məs. type='Closed')
# ---------------------------------------------------------------------------

BREAKDOWN_TEMPLATE = """select
    {select_list}
from {source_table} m
{joins}
{where_clause}
{group_by_clause}
{order_by_clause}"""

ALLOWED_OUTPUT_KINDS = ("sum", "weighted_avg", "share_of_total", "ratio", "sum_where")


def _measure_sql(model: dict, column: str, alias: str = "m") -> str:
    """Ölçü sütununa istinad — modeldə olmalıdır."""
    _, measures = _get_dims_and_measures(model)
    if column not in measures:
        raise ValueError(f"'{column}' modeldə ('{model['name']}') measure kimi tapılmadı")
    return f"{alias}.{_q(column)}"


def _dimension_sql(model: dict, column: str, alias: str = "m") -> str:
    dims, _ = _get_dims_and_measures(model)
    if column not in dims:
        raise ValueError(f"'{column}' modeldə ('{model['name']}') ölçü (dimension) deyil")
    if dims[column].get("unusable"):
        raise ValueError(f"'{column}' sütunu hazırda tam boşdur (NULL)")
    return f"{alias}.{_q(column)}"


def _round(expr: str, digits) -> str:
    if digits is None:
        return expr
    if not isinstance(digits, int) or not 0 <= digits <= 6:
        raise ValueError(f"round 0-6 aralığında tam ədəd olmalıdır, gəldi: {digits!r}")
    return f"round(({expr})::numeric, {digits})"


def build_output_sql(model: dict, spec: dict, group_by_parts: list) -> str:
    """Bir çıxış sütununun SQL ifadəsini qurur."""
    kind = spec.get("kind", "sum")
    if kind not in ALLOWED_OUTPUT_KINDS:
        raise ValueError(f"İcazə verilməyən çıxış tipi: {kind}")
    name = _safe_label(spec["name"])

    if kind == "sum":
        expr = f"sum({_measure_sql(model, spec['measure'])})"

    elif kind == "weighted_avg":
        # Sadə AVG aqreqat sətirlərdə YANLIŞDIR: hər sətir fərqli həcmi
        # təmsil edir. Ekspertin bütün SQL-lərində çəkili orta işlədilir.
        measure = _measure_sql(model, spec["measure"])
        weight = _measure_sql(model, spec["weight"])
        expr = f"sum({measure} * {weight}) / nullif(sum({weight}), 0)"

    elif kind == "share_of_total":
        measure = _measure_sql(model, spec["measure"])
        partition = spec.get("partition_by") or []
        for column in partition:
            _dimension_sql(model, column)
        over = ""
        if partition:
            columns = ", ".join(_dimension_sql(model, c) for c in partition)
            over = f"partition by {columns}"
        expr = f"100.0 * sum({measure}) / nullif(sum(sum({measure})) over ({over}), 0)"

    elif kind == "ratio":
        numerator = f"sum({_measure_sql(model, spec['numerator'])})"
        denominator = f"sum({_measure_sql(model, spec['denominator'])})"
        # Opsional: hər tərəfə CASE WHEN filtri (məs. numerator yalnız
        # type IN ('New','Reactivated') olan sətirlər üzrə cəmlənsin).
        # Bu, iki artıq-hesablanmış sum_where nəticəsinin nisbətini almaq
        # üçün lazımdır (məs. gross_outflow/gross_inflow).
        num_filter = spec.get("numerator_filter")
        if num_filter:
            column = _dimension_sql(model, num_filter["column"])
            value_list = ", ".join(_lit(v) for v in num_filter["values"])
            inner = f"abs({_measure_sql(model, spec['numerator'])})" if num_filter.get("absolute") else _measure_sql(model, spec['numerator'])
            numerator = f"sum(case when {column} in ({value_list}) then {inner} else 0 end)"
        den_filter = spec.get("denominator_filter")
        if den_filter:
            column = _dimension_sql(model, den_filter["column"])
            value_list = ", ".join(_lit(v) for v in den_filter["values"])
            inner = f"abs({_measure_sql(model, spec['denominator'])})" if den_filter.get("absolute") else _measure_sql(model, spec['denominator'])
            denominator = f"sum(case when {column} in ({value_list}) then {inner} else 0 end)"
        multiplier = "100.0 * " if spec.get("as_percentage") else ""
        expr = f"{multiplier}{numerator} / nullif({denominator}, 0)"

    else:  # sum_where
        measure = _measure_sql(model, spec["measure"])
        column = _dimension_sql(model, spec["column"])
        dims, _ = _get_dims_and_measures(model)
        allowed = dims[spec["column"]].get("allowed_values")
        values = spec.get("values") or []
        if not values:
            raise ValueError(f"'{name}': sum_where üçün 'values' boş ola bilməz")
        for value in values:
            if allowed is not None and value not in allowed:
                raise ValueError(
                    f"'{name}': '{value}' dəyəri '{spec['column']}' üçün icazə verilən "
                    f"siyahıda deyil: {allowed}"
                )
        value_list = ", ".join(_lit(v) for v in values)
        inner = f"abs({measure})" if spec.get("absolute") else measure
        expr = f"sum(case when {column} in ({value_list}) then {inner} else 0 end)"

    return f"{_round(expr, spec.get('round'))} as {_q(name)}"


def op_breakdown(model: dict, models: dict, relationships: list,
                 outputs: list, filters: list, group_by: list,
                 temporal_scope: str = "latest_snapshot",
                 time_column: str = None, granularity: str = None) -> str:
    if not outputs:
        raise ValueError("breakdown üçün 'outputs' boş ola bilməz")

    filters = apply_snapshot_default_filter(filters, model, temporal_scope)
    conditions, joins = build_where_sql(filters, model, models, relationships)

    group_by_parts, select_dims = [], []

    # Opsional aylıq/gündəlik/illik qruplaşdırma — time_column verilibsə,
    # bu, HƏMİŞƏ group_by-ın İLK sütunu kimi əlavə olunur (date_trunc ilə).
    # Bu, breakdown-un çox-measure (say+məbləğ+çəkili-orta+pay) gücünü,
    # time_series-in dövr-üzrə-ayırma gücü ilə birləşdirir.
    if time_column:
        time_col_sql, time_join = resolve_column(time_column, model, models, relationships)
        if time_join:
            joins.add(time_join)
        period_expr = f"date_trunc('{granularity or 'month'}', {time_col_sql})"
        group_by_parts.append(period_expr)
        select_dims.append(f"{period_expr} as period")

    for column in group_by or []:
        column_sql, join_sql = resolve_column(column, model, models, relationships)
        group_by_parts.append(column_sql)
        # coalesce(...) kimi ifadələr çıxışda "coalesce" adı ilə görünməsin
        select_dims.append(f"{column_sql} as {_q(column)}"
                           if column_sql != f'm.{_q(column)}' else column_sql)
        if join_sql:
            joins.add(join_sql)

    # share_of_total-un partition_by-ı GROUP BY-da olmayan sütuna istinad
    # edərsə, Postgres kriptik bir xəta verir ("must appear in GROUP BY").
    # Burada ƏVVƏLCƏDƏN, aydın mesajla tutulur — icra vaxtına qədər gözlənilmir.
    group_by_set = set(group_by or [])
    for spec in outputs:
        if spec.get("kind") == "share_of_total":
            for column in spec.get("partition_by") or []:
                if column not in group_by_set:
                    raise ValueError(
                        f"'{spec['name']}': partition_by='{column}' group_by-da yoxdur — "
                        f"bu sütun üzrə paylaşma hesablamaq üçün group_by siyahısına "
                        f"'{column}' əlavə edilməlidir"
                    )

    select_parts = list(select_dims)
    select_parts += [build_output_sql(model, spec, group_by_parts) for spec in outputs]

    return BREAKDOWN_TEMPLATE.format(
        select_list=",\n    ".join(select_parts),
        source_table=_tbl(model),
        joins="\n".join(joins),
        where_clause=("where " + " and ".join(conditions)) if conditions else "",
        group_by_clause=("group by " + ", ".join(group_by_parts)) if group_by_parts else "",
        order_by_clause=("order by " + ", ".join(group_by_parts)) if group_by_parts else "",
    )


def uses_approximate_measure(unit: dict, model: dict) -> bool:
    """
    Unit həqiqətən non-additive ölçü (CIF_countd və s.) istifadə edirmi?

    Yalnız 'allow_approximate' bayrağına baxmaq YANLIŞ olardı — bəzi unit-lər
    bayrağı daşıyır, amma çıxışlarında müştəri sayı yoxdur. Onda istifadəçiyə
    yersiz xəbərdarlıq göstərilərdi.
    """
    _, measures = _get_dims_and_measures(model)
    non_additive = {name for name, spec in measures.items() if spec.get("additive") is False}
    if not non_additive:
        return False

    used = set()
    based_on_measure = (unit.get("based_on") or {}).get("measure")
    if based_on_measure:
        used.add(based_on_measure)
    for spec in unit.get("outputs") or []:
        for key in ("measure", "numerator", "denominator", "weight"):
            if spec.get(key):
                used.add(spec[key])
    return bool(used & non_additive)


def compile_query(semantic_query: dict) -> str:
    models = load_models()
    metrics = load_metrics()
    relationships = load_relationships()

    metric_name = semantic_query["metric"]
    metric_def = metrics[metric_name]
    model = models[metric_def["based_on"]["model"]]
    measure_column = metric_def["based_on"].get("measure")
    op_type = metric_def["operation"]

    user_filters = semantic_query.get("filters", [])
    group_by = semantic_query.get("group_by", [])

    if op_type == "aggregate":
        base_filters = metric_def.get("base_filters", [])
        count_distinct = metric_def.get("count_distinct")
        temporal_scope = metric_def.get("temporal_scope", "latest_snapshot")
        return op_aggregate(
            model, models, relationships,
            measure_column=measure_column,
            filters=base_filters + user_filters,
            group_by=group_by,
            metric_name=metric_name,
            count_distinct_column=count_distinct,
            temporal_scope=temporal_scope,
        )

    elif op_type == "share":
        base_subgroup_filters = metric_def.get("subgroup_filters", [])
        base_total_filters = metric_def.get("total_filters", [])
        temporal_scope = metric_def.get("temporal_scope", "latest_snapshot")
        # scope_filters: referens çərçivəsi — HƏM subgroup, HƏM total-a aid olur
        # (məs. "saving müştəriləri arasında" — hər ikisi yalnız saving-lərə baxmalıdır)
        scope_filters = semantic_query.get("scope_filters", [])
        # subgroup_filters (user): YALNIZ subgroup-u əlavə fərqləndirən şərt
        # (məs. "balansı 100K-dan çox olanlar")
        extra_subgroup_filters = semantic_query.get("subgroup_filters", [])
        return op_share(
            model, models, relationships,
            measure_column=measure_column,
            subgroup_filters=base_subgroup_filters + scope_filters + extra_subgroup_filters,
            total_filters=base_total_filters + scope_filters,
            group_by=group_by,
            metric_name=metric_name,
            temporal_scope=temporal_scope,
        )

    elif op_type == "share_case_when":
        temporal_scope = metric_def.get("temporal_scope", "latest_snapshot")
        return op_share_case_when(
            model, models, relationships,
            measure_column=measure_column,
            case_column=metric_def["case_column"],
            case_value_a=metric_def["case_value_a"],
            case_value_b=metric_def["case_value_b"],
            label_a=metric_def["label_a"],
            label_b=metric_def["label_b"],
            filters=user_filters,
            group_by=group_by,
            metric_name=metric_name,
            temporal_scope=temporal_scope,
        )

    elif op_type == "compare":
        base_a = metric_def.get("period_a_filters", [])
        base_b = metric_def.get("period_b_filters", [])
        # LLM runtime-da DİNAMİK dövr göndərə bilər (məs. "2023 ilin oktyabrı
        # ilə 2022-ni müqayisə et") — bu, units.yml-dəki SABİT period_a/b
        # filters-dən FƏRQLİDİR. LLM-in göndərdiyi varsa, o, sabit dəyərin
        # ÜSTÜNƏ deyil, ƏVƏZİNƏ keçir (əks halda iki fərqli tarix aralığı
        # ziddiyyətli WHERE şərti yaradıb, heç bir sətir qaytarmazdı).
        dynamic_a = semantic_query.get("period_a_filters")
        dynamic_b = semantic_query.get("period_b_filters")
        period_a_filters = dynamic_a if dynamic_a is not None else (base_a + user_filters)
        period_b_filters = dynamic_b if dynamic_b is not None else (base_b + user_filters)
        return op_compare(
            model, models, relationships,
            measure_column=measure_column,
            period_a_filters=period_a_filters,
            period_b_filters=period_b_filters,
            period_a_label=metric_def.get("period_a_label", "period_a"),
            period_b_label=metric_def.get("period_b_label", "period_b"),
            metric_name=metric_name,
            group_by=group_by,
        )

    elif op_type == "breakdown":
        base_filters = metric_def.get("base_filters", [])
        return op_breakdown(
            model, models, relationships,
            outputs=metric_def.get("outputs", []),
            filters=base_filters + user_filters,
            group_by=group_by,
            temporal_scope=metric_def.get("temporal_scope", "latest_snapshot"),
            time_column=metric_def.get("time_column"),
            granularity=metric_def.get("granularity"),
        )

    elif op_type == "time_series":
        base_filters = metric_def.get("base_filters", [])
        # Yalnız istifadəçi açıq istəyəndə true olur (Infer Agent-dən gəlir).
        include_current = bool(semantic_query.get("include_current_period", False))
        return op_time_series(
            model, models, relationships,
            measure_column=measure_column,
            filters=base_filters + user_filters,
            time_column=metric_def["time_column"],
            granularity=metric_def.get("granularity", "month"),
            metric_name=metric_name,
            include_current_period=include_current,
            group_by=group_by,
        )

    else:
        raise ValueError(f"Naməlum əməliyyat tipi: {op_type}")