"""
Semantic Layer Compiler v2 — təkrar istifadə olunan ÜMUMİ ƏMƏLİYYATLAR:

  1. AGGREGATE — bir measure-u (filters + group_by ilə) cəmləmək/saymaq/ortalamaq
  2. SHARE      — bir alt-qrupun bütövə nisbəti (AGGREGATE-in üstündə)
  3. COMPARE    — iki filtr toplusunun (iki dövr, iki kanal, s.) müqayisəsi
  4. TIME SERIES — flow məlumatının gün/ay/kvartal/il trendi
  5. SNAPSHOT TIME SERIES — stock məlumatında hər dövrün son snapshot trendi
  6. BREAKDOWN  — bir neçə təsdiqlənmiş çıxışla ölçülər üzrə bölgü
  7. RANKED BREAKDOWN — cari və ya hər dövr daxilində top/bottom sıralama

Hər unit (units.yml-də) bu əməliyyatlardan BİRİNİN parametrləşdirilmiş
çağırışıdır — yeni sual tipi üçün yeni SQL YAZILMIR, mövcud əməliyyat
fərqli parametrlərlə çağırılır.

LLM heç vaxt bu faylı görmür/yazmır — o, yalnız semantic_query JSON-unu doldurur.
"""
import glob
import os
import re

import yaml
from . import domain_policy

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# GREENPLUM UYĞUNLAŞDIRMASI (PoC)
#
# Bu fayl semantic_poc/compiler.py-dən götürülüb. Mühərrik dəyişməyib —
# yalnız aşağıdakı hədəf-spesifik düzəlişlər edilib:
#
#   1. İDENTİFİKATOR DIRNAQLARI — sütun adları dırnağa alınır. Datamart
#      07.09.2026-da kiçik hərfə keçirilib, amma cədvəl adı hələ də qarışıq
#      registrlidir ("AIDASHBOARD_dm_deposit_portfolio_ai") və dırnaqsız
#      PostgreSQL onu kiçik hərfə salıb tapa bilmir.
#   2. round(x, 2) -> round(x::numeric, 2) — PostgreSQL-də double precision
#      üçün iki arqumentli round YOXDUR (işləmə zamanı xəta verir).
#   3. Sətir dəyərləri kaçırılır (' -> '') — SQL injection səthini bağlayır.
#   4. overlap əməliyyatı SİLİNİB — müştəri səviyyəsində açar tələb edir,
#      aqreqasiya olunmuş datamartda belə açar yoxdur.
#   5. Measure sütunlarına adi WHERE filtri QADAĞANDIR — aqreqat sətirdə
#      "balansı 100 000-dən çox müştəri" kimi şərt yanlış nəticə verir.
#      Ekspertin təsdiqlədiyi measure-lər yalnız conditional output daxilində
#      model metadata-sındakı `conditional_filter` icazəsi ilə işləyir.
#   6. Non-additive measure-lər (cif_countd və s.) sətirlər üzrə cəmlənə
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
LATEST_DATE = "__LATEST_DATE__"
CURRENT_MONTH = "__CURRENT_MONTH__"
TREND_OPERATIONS = ("time_series", "snapshot_time_series")


class ParameterValidationError(ValueError):
    """LLM parameter payload is invalid; bounded re-inference may repair it."""


def is_trend_unit(unit: dict) -> bool:
    return (unit.get("operation") in TREND_OPERATIONS
            or (unit.get("operation") == "ranked_breakdown" and bool(unit.get("time_column"))))


def latest_date_sql(model: dict) -> str:
    column = find_time_dimension(model)
    if not column:
        raise ValueError("Modelin zaman sütunu yoxdur")
    return f"(select max({_q(column)}) from {_tbl(model)})"


def relative_period_sql(value: str) -> str:
    """
    'last_n_months:12' -> son 12 tam ayın başlanğıcı
    'last_n_days:30'   -> bugündən 30 gün əvvəl

    Yalnız bu iki forma. Sərbəst tarix arifmetikası qəbul edilmir.
    """
    if value == CURRENT_MONTH:
        return "date_trunc('month', current_date)"
    match = RELATIVE_PERIOD.match(str(value))
    if not match:
        raise ParameterValidationError(
            f"Yararsız nisbi dövr: {value!r} "
            f"(gözlənilir: last_n_months:N və ya last_n_days:N)"
        )
    unit, amount = match.group(1), int(match.group(2))
    if unit == "months":
        if not 1 <= amount <= MAX_RELATIVE_MONTHS:
            raise ParameterValidationError(f"Ay sayı 1-{MAX_RELATIVE_MONTHS} aralığında olmalıdır, gəldi: {amount}")
        return f"(date_trunc('month', current_date) - interval '{amount} months')"
    if not 1 <= amount <= MAX_RELATIVE_DAYS:
        raise ParameterValidationError(f"Gün sayı 1-{MAX_RELATIVE_DAYS} aralığında olmalıdır, gəldi: {amount}")
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


def _same_column(a, b) -> bool:
    """
    İki sütun adı eynidirmi — REGİSTRDƏN ASILI OLMAYARAQ.

    Infer Agent tarix sütununu köhnə registrlə göndərə bilər ("BANK_DATE").
    Sadə == müqayisəsi bunu görmür: açıq dövr TAPILMIR, üstündən bir də
    default dövr qoyulur və eyni sorğuda iki tarix şərti qalır.
    """
    return isinstance(a, str) and isinstance(b, str) and a.lower() == b.lower()


def _period_filter_slots(operation: str) -> list:
    """Bu əməliyyat üçün tarix şərtinin ola biləcəyi filtr siyahıları."""
    if operation == "share":
        return ["scope_filters", "subgroup_filters"]
    if operation == "compare":
        return ["period_a_filters", "period_b_filters"]
    return ["filters"]


# Dövr BİR sətir kimi daşınır: effective_period["value"], sub["period_value"],
# fallback-a ötürülən hədd — hamısı sadə string müqayisəsi ilə işləyir.
# İKİ HƏDDLİ pəncərə ona görə ayırıcı ilə kodlanır:
#
#     "last_n_months:12|<last_n_months:6"
#      └ aşağı hədd (>=)  └ yuxarı həddin OPERATORU + dəyəri
#
# ÖLÇÜLÜB (16.09.2026, qəbul sualı 1): period_value_of() UYĞUN GƏLƏN BİRİNCİ
# filtri qaytarırdı, yəni "12 ay əvvəldən 6 ay əvvələdək" pəncərəsi
# "last_n_months:12" kimi normallaşırdı — YUXARI HƏDD İTİRDİ. Həmin dəyəri
# miras alan növbəti alt-sual son 12 AYIN HAMISINI götürürdü, birincinin
# işlətdiyi 12→6 dilimini yox. İki rəqəm eyni cavabda yan-yana düşür,
# etiketlər isə hər ikisini "eyni dövr" kimi göstərir.
PERIOD_RANGE_SEP = "|"
_UPPER_OPERATORS = ("<=", "<")


def split_period_value(period_value):
    """
    Dövr dəyərini hissələrinə ayırır: (aşağı_hədd, yuxarı_operator, yuxarı_hədd).

    Tək həddli dəyərdə son iki element None-dur. Yalnız yuxarı həddi olan
    pəncərədə aşağı hədd boş sətirdir — apply_period() onda tək "<" şərti
    yazır (əvvəl belə pəncərə səhvən ">=" şərtinə çevrilirdi).
    """
    if not isinstance(period_value, str) or PERIOD_RANGE_SEP not in period_value:
        return period_value, None, None
    lower, _, upper = period_value.partition(PERIOD_RANGE_SEP)
    for operator in _UPPER_OPERATORS:
        if upper.startswith(operator):
            return lower, operator, upper[len(operator):]
    return lower, "<", upper


def accepts_period(unit: dict, model: dict) -> bool:
    """
    Bu unit-ə tarix aralığı tətbiq oluna bilərmi?

    Snapshot modellərin trend olmayan unit-ləri XEYR — onlarda dövr "ən son
    gün"dür, aralıq deyil; apply_snapshot_default_filter bunu idarə edir.
    Trend unit-ləri tarix filtrini qəbul edir: istifadəçinin açıq ili/aralığı
    və ya unit-in sənədləşdirilmiş default pəncərəsi trendi məhdudlaşdıra bilər.
    """
    if unit.get("operation") == "compare":
        # compare: iki dövrü ÖZÜ təyin edir — üçüncü filtr əlavə etmək
        # mənasızdır və op_compare onu onsuz da "filters" slotunda görməz.
        return False
    if model.get("snapshot_date_column") and not is_trend_unit(unit):
        return False
    return find_time_dimension(model) is not None


def has_explicit_period(semantic_query: dict, unit: dict, model: dict) -> bool:
    """semantic_query-də zaman sütunu üzrə şərt varmı?"""
    time_col = find_time_dimension(model)
    if not time_col:
        return False
    for slot in _period_filter_slots(unit.get("operation", "aggregate")):
        for f in semantic_query.get(slot) or []:
            if isinstance(f, dict) and _same_column(f.get("column"), time_col):
                return True
    return False


def without_period_filters(semantic_query: dict, unit: dict, model: dict) -> dict:
    """Return the query without filters on the model's time dimension."""
    time_col = find_time_dimension(model)
    if not time_col:
        return semantic_query
    result = dict(semantic_query)
    for slot in _period_filter_slots(unit.get("operation", "aggregate")):
        if slot not in result:
            continue
        kept = [
            item for item in result.get(slot) or []
            if not (isinstance(item, dict) and _same_column(item.get("column"), time_col))
        ]
        if kept:
            result[slot] = kept
        else:
            result.pop(slot, None)
    return result


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
    lower, upper_operator, upper = split_period_value(period_value)

    # İki həddli pəncərə İKİ şərt kimi yazılır — yuxarı həddi atmaq
    # "12→6 ay əvvəl" dilimini "son 12 ay"a çevirərdi.
    added = []
    if lower:
        added.append({"column": time_col, "operator": ">=", "value": lower})
    if upper:
        added.append({"column": time_col, "operator": upper_operator, "value": upper})
    if not added:
        return semantic_query

    result = dict(semantic_query)
    result[target_slot] = list(result.get(target_slot) or []) + added
    return result


def apply_default_period(semantic_query: dict, unit: dict, model: dict) -> dict:
    """İstifadəçi dövr verməyibsə, unit-in 'default_period'-unu tətbiq edir."""
    return apply_period(semantic_query, unit, model, unit.get("default_period"))


def period_value_of(semantic_query: dict, unit: dict, model: dict):
    """
    semantic_query-dəki tarix şərtinin DƏYƏRİ (müqayisə üçün normallaşdırılmış).
    Yoxdursa None. Snapshot modellərdə "__snapshot__" qaytarır — bu, aralıq
    deyil, ayrıca bir dövr NÖVÜDÜR və aralıqlarla müqayisə edilməməlidir.

    HƏR İKİ hədd varsa nəticə birləşdirilmiş dəyərdir (bax PERIOD_RANGE_SEP) —
    yalnız birincisini qaytarmaq pəncərəni səssizcə genişləndirirdi.
    """
    if model.get("snapshot_date_column") and not is_trend_unit(unit):
        return "__snapshot__"
    time_col = find_time_dimension(model)
    if not time_col:
        return None

    lower, upper = None, None
    for slot in _period_filter_slots(unit.get("operation", "aggregate")):
        for f in semantic_query.get(slot) or []:
            if not (isinstance(f, dict) and _same_column(f.get("column"), time_col)):
                continue
            operator = str(f.get("operator") or ">=").strip()
            if operator in _UPPER_OPERATORS:
                if upper is None:
                    upper = (operator, str(f.get("value")))
            elif lower is None:
                lower = str(f.get("value"))

    if upper is None:
        return lower
    return f"{lower or ''}{PERIOD_RANGE_SEP}{upper[0]}{upper[1]}"


def period_kind(semantic_query: dict, unit: dict, model: dict) -> str:
    """
    Dövrün NÖVÜ — uyğunluq yoxlaması üçün.

      snapshot  — "ən son gün" (stock cədvəl); aralıqla müqayisə edilmir
      range     — konkret tarix aralığı
      unbounded — heç bir tarix həddi yoxdur (bütün tarix)

    'trend' ayrıca sayılır: time_series bütün ayları göstərməlidir, ona görə
    həddsiz olması qüsur deyil.
    """
    if is_trend_unit(unit):
        return "trend"
    if model.get("snapshot_date_column"):
        return "snapshot"
    return "range" if period_value_of(semantic_query, unit, model) else "unbounded"


def _describe_single_filter(f: dict) -> str:
    value = f.get("value")
    if value == CURRENT_MONTH:
        return "bu ay"
    if value == LATEST_DATE:
        return "ən son mövcud tarix (daxil)"
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
    # REGİSTRDƏN ASILI OLMAYAN müqayisə: hər yerdə _same_column() işlədilir,
    # burada isə sadə == qalmışdı. Agent "BANK_DATE" göndərsə, tarix şərti
    # TAPILMIR və etiket "bütün tarixi məlumat" olur — halbuki SQL-də hədd var.
    matches = [f for f in filters or []
               if isinstance(f, dict) and _same_column(f.get("column"), time_col)]
    if not matches:
        return ""
    if len(matches) == 1:
        return _describe_single_filter(matches[0])
    starts = [m for m in matches if m.get("operator") in (">=", ">")]
    ends = [m for m in matches if m.get("operator") in ("<", "<=")]
    if starts and ends:
        # Aralıq bağlıdır — başlanğıcı "...dən indiyədək" kimi açıq
        # oxutmaq YANLIŞDIR (indiyədək deyil, son tarixədək davam edir).
        start_value = str(starts[0].get("value"))
        end_value = str(ends[0].get("value"))
        start_match = RELATIVE_PERIOD.match(start_value)
        end_match = RELATIVE_PERIOD.match(end_value)
        # Hər iki hədd nisbidirsə (compare-in "əvvəlki dövr"ü) xam dəyər
        # sızmamalıdır: "son 12 ay – last_n_months:6" oxunmurdu.
        if start_match and end_match:
            word = "ay" if start_match.group(1) == "months" else "gün"
            return (f"{start_match.group(2)} {word} əvvəldən "
                    f"{end_match.group(2)} {word} əvvələdək")
        start_text = _describe_single_filter(starts[0]) if start_match else start_value
        end_text = _describe_single_filter(ends[0]) if end_match or end_value == LATEST_DATE else end_value
        if ends[0].get("operator") == "<=" and end_value != LATEST_DATE:
            end_text += " (daxil)"
        elif ends[0].get("operator") == "<" and re.fullmatch(r"\d{4}-\d{2}-\d{2}", end_value):
            end_text += " (daxil deyil)"
        return f"{start_text} – {end_text}"
    return "; ".join(_describe_single_filter(m) for m in matches)


def describe_period_value(period_value: str) -> str:
    """
    Dövr DƏYƏRİNİ ("last_n_months:6") azərbaycanca yazır ("son 6 ay").

    describe_period() unit və model tələb edir; dinamik SQL-in isə unit-i
    YOXDUR. Etiket orada da göstərilməlidir — dövrü tətbiq edib adını
    deməmək məhz gizli fərqdir.
    """
    lower, upper_operator, upper = split_period_value(period_value)
    parts = []
    if lower:
        parts.append(_describe_single_filter({"value": lower}))
    if upper:
        parts.append(_describe_single_filter({"operator": upper_operator,
                                              "value": upper}) + "-dək")
    return " ".join(parts) or "dövr həddi yoxdur"


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

    if is_trend_unit(unit):
        grain = unit.get("granularity", "month")
        period_word = "gün" if grain == "day" else "il" if grain == "year" else "ay"
        period_plural = {"day": "günlər", "year": "illər", "month": "aylar"}.get(
            grain, "aylar")
        current = (f"cari {period_word} daxildir" if semantic_query.get("include_current_period")
                   else f"cari {period_word} xaric tutulur")
        base = found or f"bütün mövcud {period_plural} üzrə"
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


def _check_required_filters(semantic_query: dict, unit: dict, model: dict) -> None:
    """
    MƏCBURİ filtr sütunları verilibmi?

    group_by-dan FƏRQLİ olaraq burada avtomatik əlavə etmək OLMAZ: filtrin
    dəyəri lazımdır və onu uydurmaq nəticəni səssizcə dəyişərdi.

    ÖLÇÜLÜB (eyni defekt sinfi): satis_dovr_muqayisesi hər iki dövr yuvasında
    'tarix' tələb edir. Agent onu buraxsa, heç nə əlavə etmir və hər iki SELECT
    filtrsiz qalır — nəticə iki EYNİ bütün-tarix sətri olur və cavab
    "dəyişiklik yoxdur" kimi səssizcə YANLIŞ çıxır. Ona görə burada dayanılır:
    bu, ADR-0008-in məhdud təkrar sorğusunu işə salır.
    """
    for slot, spec in (unit.get("parameters") or {}).items():
        if slot == "group_by":
            continue          # məcburi qruplaşdırma avtomatik əlavə olunur
        required = (spec or {}).get("required") or []
        if not required:
            continue
        given = {str(f.get("column", "")).lower()
                 for f in semantic_query.get(slot) or [] if isinstance(f, dict)}
        for column in required:
            real = real_column_name(column, model)
            if real.lower() not in given:
                raise ParameterValidationError(
                    f"'{slot}' yuvasında '{real}' MƏCBURİDİR, verilməyib. "
                    f"Bu unit onsuz mənasız nəticə verir "
                    f"(zaman sütunu üçün: 'last_n_months:N' və ya YYYY-MM-DD)"
                )


def _check_required_group_by(group_by: list, unit: dict, model: dict) -> None:
    """Reject a proposal that omits a trusted unit's required row grain."""
    required = ((unit.get("parameters") or {}).get("group_by") or {}).get("required") or []
    if not required:
        return
    present = {str(c).lower() for c in group_by}
    missing = []
    for column in required:
        real = real_column_name(column, model)
        if real.lower() not in present:
            missing.append(real)
    if missing:
        raise ParameterValidationError(
            "Infer Agent unit-in məcburi group_by sütunlarını qaytarmayıb: "
            + ", ".join(missing)
        )


def real_column_name(col_name: str, model: dict) -> str:
    """
    Modeldəki HƏQİQİ sütun adı — registr fərqi normallaşdırılır.

    resolve_column SQL ifadəsi üçün bunu onsuz da edir, amma group_by adı həm də
    ÇIXIŞ SÜTUNUNUN ADI kimi və share_of_total-un partition yoxlamasında
    işlədilir. Datamart 07.09.2026-da kiçik hərfə keçəndən sonra model
    'product_code' saxlayır, LLM isə hələ də 'PRODUCT_CODE' göndərə bilir:
    normallaşdırma olmasa SQL 'product_code' üzrə qruplaşır, yoxlama isə
    'PRODUCT_CODE' axtarır və əsassız xəta verir.
    """
    dims, measures = _get_dims_and_measures(model)
    if col_name in dims or col_name in measures:
        return col_name
    lower_map = {c.lower(): c for c in list(dims) + list(measures)}
    return lower_map.get(str(col_name).lower(), col_name)


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


def canonical_number(value):
    """Return a numeric JSON scalar for an unambiguous numeric string."""
    if isinstance(value, bool) or not isinstance(value, str):
        return value
    text = value.strip()
    if not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", text):
        return value
    number = float(text)
    return int(number) if number.is_integer() else number


def _is_date_like(value) -> bool:
    text = str(value)
    if text == CURRENT_MONTH or RELATIVE_PERIOD.match(text):
        return True
    return bool(re.match(r"^\d{4}-\d{2}(-\d{2})?", text))


def validate_filter(filter_item: dict, model: dict) -> tuple:
    column = filter_item["column"]
    value = filter_item["value"]
    operator = filter_item.get("operator", "=")

    # Operator böyük/kiçik hərfə həssas olmasın (LLM "in" yoxsa "IN" göndərə bilər)
    operator = operator.upper() if operator.lower() == "in" else operator

    if operator not in ("=", "!=", ">", "<", ">=", "<=", "IN"):
        raise ParameterValidationError(f"İcazə verilməyən operator: {operator}")

    dims, measures = _get_dims_and_measures(model)

    # Case-insensitive uyğunlaşdırma: LLM sütun adını fərqli böyük/kiçik
    # hərflə yazsa (interest_rate vs interest_rate), real adı tapırıq
    if column not in dims and column not in measures:
        column_lower_map = {c.lower(): c for c in list(dims.keys()) + list(measures.keys())}
        real_column = column_lower_map.get(column.lower())
        if real_column is None:
            # İcazə verilən adlar da göstərilir: bu mesaj Infer Agent-ə geri
            # verilir (ADR-0008, bounded re-ask) və düzəliş üçün ona lazımdır.
            raise ParameterValidationError(
                f"Filter sütunu '{column}' modeldə ('{model['name']}') tapılmadı. "
                f"Mövcud sütunlar: {', '.join(sorted(dims))}"
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

    if value == LATEST_DATE:
        if column != find_time_dimension(model) or operator != "<=":
            raise ParameterValidationError("Ən son tarix yalnız zaman sütununun daxil olan yuxarı həddi ola bilər")
        return column, operator, value

    # TİP YOXLAMASI. Enum siyahısı olmayan sütunlarda (məs. 39 fərqli 'term')
    # əvvəllər heç bir dəyər yoxlanışı yox idi: model "term = 'Son 12 ay'"
    # göndərdi, sətir bigint sütuna düşdü və sorğu İCRA VAXTI qırıldı.
    # İndi bu, kompilyasiya mərhələsində, aydın mesajla dayandırılır.
    if column in dims:
        data_type = dims[column].get("data_type")
        if data_type == "number":
            if isinstance(value, list):
                value = [canonical_number(candidate) for candidate in value]
            else:
                value = canonical_number(value)
        for candidate in (value if isinstance(value, list) else [value]):
            if data_type == "number" and not _is_numeric(candidate):
                raise ParameterValidationError(
                    f"'{column}' rəqəm sütunudur, '{candidate}' isə mətndir. "
                    f"Tarix aralığı üçün rəqəm sütununu istifadə etmək olmaz — "
                    f"zaman sütununda 'last_n_months:N' yazın"
                )
            if data_type == "date" and not _is_date_like(candidate):
                raise ParameterValidationError(
                    f"'{column}' tarix sütunudur, '{candidate}' uyğun deyil. "
                    f"Qəbul olunur: YYYY-MM-DD və ya 'last_n_months:N'"
                )

    if column in dims:
        allowed = dims[column].get("allowed_values")
        if allowed is not None:
            values_to_check = value if isinstance(value, list) else [value]
            for v in values_to_check:
                if v not in allowed:
                    raise ParameterValidationError(
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
    (is_vip vs is_vip), real adı tapır.
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
        # is_vip kimi sütunlarda NULL real biznes mənası daşıyır ("VIP deyil").
        # Qruplaşdırmada NULL ayrıca səbət yaratmasın deyə modeldə elan olunan
        # etiketə çevrilir — ekspertin SQL-lərindəki coalesce(...) ilə eyni.
        if col_name in dims and dims[col_name].get("null_as") is not None:
            expr = f"coalesce({expr}, {_lit(dims[col_name]['null_as'])})"
        return expr, None

    for rel in relationships or []:
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

    raise ParameterValidationError(
        f"Sütun '{col_name}' heç bir modeldə tapılmadı (model: {model['name']})")


MONTH_PERIOD = re.compile(r"^last_n_months:\d{1,4}$")


def close_month_periods(filters: list, alias: str = "m") -> list:
    """
    "son N ay" filtrinə YUXARI hədd əlavə edir: son N TAM ay.

    ÖLÇÜLÜB: "son 6 ayın satışlarını əvvəlki 6 ayla müqayisə et" —
      son_dovr    >= 2026-03-01                       -> 6 ay + 7 gün
      onceki_dovr >= 2025-09-01 və < 2026-03-01       -> tam 6 ay
    Yəni müqayisə BƏRABƏR OLMAYAN pəncərələr arasında gedirdi və artım
    şişirdilmiş görünürdü.

    Domen ekspertinin öz SQL-lərində də konvensiya budur (bax
    inputs/q_a_deposit.md, Q1):
        tarix >= date_trunc('month', current_date) - 365
        and tarix <  date_trunc('month', current_date)

    Yalnız AYLIQ nisbi dövrə tətbiq olunur — "son 30 gün" təbii olaraq bu günü
    əhatə edir. Həmin sütunda artıq yuxarı hədd varsa TOXUNULMUR (məs. compare
    şablonunun ikinci dövrü onsuz da bağlıdır).
    """
    result = list(filters or [])
    bounded = {str(f.get("column")).lower() for f in result
               if isinstance(f, dict) and f.get("operator") in ("<", "<=", "between")}
    for f in list(result):
        if not isinstance(f, dict) or f.get("operator") not in (">=", ">"):
            continue
        if not MONTH_PERIOD.match(str(f.get("value", ""))):
            continue
        column = str(f.get("column"))
        if column.lower() in bounded:
            continue
        result.append({"column": column, "operator": "<",
                       "value": "__CURRENT_MONTH_START__"})
        bounded.add(column.lower())
    return result


def build_where_sql(filters: list, model: dict, models: dict, relationships: list,
                    alias: str = "m", close_periods: bool = True) -> tuple:
    """
    Filtrləri doğrulayır və WHERE bəndinə çevirir.
    Qaytarır: (şərtlər_siyahısı, əlavə_join_sql_seti)

    Xüsusi qeyd: value "__MAX__:table.column" formatındadırsa, bu, sabit
    dəyər deyil — "= (SELECT MAX(column) FROM table)" alt-sorğusuna çevrilir
    (apply_snapshot_default_filter tərəfindən yaradılır, validate_filter-dən keçmir).
    """
    conditions = []
    joins = set()
    for f in (close_month_periods(filters, alias) if close_periods else (filters or [])):
        # Compiler-in özünün əlavə etdiyi hədd — model dəyəri deyil, ona görə
        # tip/enum yoxlamasından keçmir (__MAX__ ilə eyni məntiq).
        if f.get("value") == "__CURRENT_MONTH_START__":
            column = real_column_name(f["column"], model)
            conditions.append(
                f"{alias}.{_q(column)} < date_trunc('month', current_date)")
            continue
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
                raise ParameterValidationError(
                    f"'IN' operatoru üçün 'value' siyahı (list) olmalıdır, gəldi: {type(value)}")
            formatted_values = ", ".join(_lit(v) for v in value)
            conditions.append(f"{col_sql} in ({formatted_values})")
        elif value == LATEST_DATE:
            conditions.append(f"{col_sql} <= {latest_date_sql(model)}")
        elif isinstance(value, str) and (value == CURRENT_MONTH or RELATIVE_PERIOD.match(value)):
            conditions.append(f"{col_sql} {operator} {relative_period_sql(value)}")
        else:
            conditions.append(f"{col_sql} {operator} {_lit(value)}")
    return conditions, joins


def row_count_label(model: dict) -> str:
    """
    Say sütununun ÇIXIŞ ADI.

    R1 düzəlişindən sonra bu sütun sətirləri yox, MÜQAVİLƏLƏRİ sayır — amma adı
    "row_count" qalmışdı. Ölçülüb: dərin təhlil onu "əməliyyat sayı" kimi oxudu
    (təsadüfən doğru), istifadəçi isə cədvəldə "row_count" görürdü.
    Modeldə müqavilə ölçüsü yoxdursa (məs. flow cədvəli) ad DƏYİŞMİR — orada
    ifadə həqiqətən count(*)-dır və "contract_count" YALAN olardı.
    """
    return "contract_count" if model.get("row_count_measure") else "row_count"


def row_count_expr(model: dict, alias: str = "m") -> str:
    """
    "Neçə ədəd?" sualının DOĞRU ifadəsi.

    Aqreqasiya olunmuş cədvəldə bir sətir bir müqavilə DEYİL — ölçü
    kombinasiyasıdır. Ona görə count(*) sətirləri sayır, müqavilələri yox.
    Model 'row_count_measure' elan edibsə, həqiqi say onun cəmidir.

    Ölçülüb (satış cədvəli): count(*) = 25,057 sətir, amma
    sum("contract_ref_no_count") = 407,052 müqavilə — 16 dəfə fərq.
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
            # Non-additive measure (məs. cif_countd) sətirlər üzrə cəmlənəndə
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


RANGE_OPERATORS = (">=", ">", "<=", "<", "between")


def check_snapshot_range(semantic_query: dict, unit: dict, model: dict) -> None:
    """
    Snapshot cədvəlində tarix ARALIĞI QADAĞANDIR.

    Snapshot cədvəlinin hər sətri bir günün TAM mənzərəsidir; günlər üzrə
    cəmlənə bilməz. apply_snapshot_default_filter tarix filtri YOXDURSA ən son
    günü qoyur — amma filtr VARSA toxunmurdu, və aralıq verildikdə hər gün
    cəmlənirdi.

    ÖLÇÜLÜB (ekspertin Q4 sualı): model
        bank_date >= last_n_months:12 AND bank_date < '2023-10-15'
    verdi; snapshot qoruması işə düşmədi. Dinamik SQL də eyni səhvi etdi və
    aylıq "portfel" 20.50 mlrd AZN göstərdi — həqiqi portfel 2.15 mlrd-dır,
    yəni ~10 dəfə şişirdilmiş rəqəm CAVABDA FAKT KİMİ verildi.

    Bərabərlik (bir konkret gün) və compiler-in öz "__MAX__" filtri qalır.
    """
    snapshot_col = model.get("snapshot_date_column")
    if not snapshot_col:
        return
    # snapshot_time_series aralığı təhlükəsiz şəkildə emal edir: əvvəlcə hər
    # dövrün ən son mövcud snapshot tarixini seçir, yalnız sonra məbləği
    # aqreqasiya edir. Pilot logundakı aylıq portfel sorğuları bu xüsusi
    # əməliyyat olmadan gündəlik stock-ları cəmləyib şişirdirdi.
    if (unit.get("operation") == "snapshot_time_series"
            or (unit.get("operation") == "ranked_breakdown" and unit.get("time_column"))):
        return
    for slot in ("filters", "scope_filters", "subgroup_filters",
                 "period_a_filters", "period_b_filters"):
        for f in semantic_query.get(slot) or []:
            if not isinstance(f, dict):
                continue
            if not _same_column(f.get("column"), snapshot_col):
                continue
            value = f.get("value")
            if isinstance(value, str) and value.startswith("__MAX__:"):
                continue
            if str(f.get("operator", "=")).lower() in RANGE_OPERATORS:
                raise ValueError(
                    f"'{snapshot_col}' snapshot (gün mənzərəsi) sütunudur — "
                    f"tarix ARALIĞI verilə bilməz, çünki günlər cəmlənir və "
                    f"nəticə dəfələrlə şişirdilmiş olur. Ya heç bir tarix "
                    f"filtri vermə (ən son gün avtomatik seçilir), ya da bir "
                    f"konkret gün üçün '=' işlət (məs. '2026-03-31')."
                )


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
                  temporal_scope: str = "latest_snapshot", close_periods: bool = True) -> str:
    agg_func = get_default_agg(model, measure_column)

    filters = apply_snapshot_default_filter(filters, model, temporal_scope)
    conditions, joins = build_where_sql(filters, model, models, relationships, close_periods=close_periods)

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
             group_by: list, metric_name: str, temporal_scope: str = "latest_snapshot", close_periods: bool = True) -> str:
    agg_func = get_default_agg(model, measure_column)

    subgroup_filters = apply_snapshot_default_filter(subgroup_filters, model, temporal_scope)
    total_filters = apply_snapshot_default_filter(total_filters, model, temporal_scope)

    subgroup_conditions, joins1 = build_where_sql(subgroup_filters, model, models, relationships, close_periods=close_periods)
    total_conditions, joins2 = build_where_sql(total_filters, model, models, relationships, close_periods=close_periods)
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
                        temporal_scope: str = "latest_snapshot", close_periods: bool = True) -> str:
    filters = apply_snapshot_default_filter(filters, model, temporal_scope)
    conditions, joins = build_where_sql(filters, model, models, relationships, close_periods=close_periods)
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
    {row_count_expr} as {count_label}{bounds_a}
from {source_table} m
{joins}
{where_a}
{group_by_clause}

union all

select
    '{period_b_label}' as period_label,
    {group_by_select}
    {agg_func}(m.{measure_column}) as period_value,
    {row_count_expr} as {count_label}{bounds_b}
from {source_table} m
{joins}
{where_b}
{group_by_clause}

order by period_label{order_by_extra}"""


def op_compare(model: dict, models: dict, relationships: list,
               measure_column: str, period_a_filters: list, period_b_filters: list,
               period_a_label: str, period_b_label: str, metric_name: str,
               group_by: list = None, close_periods: bool = True,
               report_period_bounds: bool = False) -> str:
    agg_func = get_default_agg(model, measure_column)

    conditions_a, joins1 = build_where_sql(period_a_filters, model, models, relationships, close_periods=close_periods)
    conditions_b, joins2 = build_where_sql(period_b_filters, model, models, relationships, close_periods=close_periods)
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

    def bounds(filters):
        if not report_period_bounds:
            return ""
        column = find_time_dimension(model)
        parts = []
        for name, operators in (("start", (">=", ">")), ("end", ("<=", "<"))):
            bound = next((f for f in filters if str(f.get("column", "")).lower() == column.lower()
                          and f.get("operator") in operators), None)
            if not bound:
                raise ParameterValidationError("Müqayisə dövrünün sərhədi yoxdur")
            value = bound["value"]
            expr = latest_date_sql(model) if value == LATEST_DATE else (
                relative_period_sql(value) if RELATIVE_PERIOD.fullmatch(str(value)) else _lit(value))
            parts.append(f"cast({expr} as date) as __period_{name}")
        return ",\n    " + ",\n    ".join(parts)

    return COMPARE_TEMPLATE.format(
        row_count_expr=row_count_expr(model),
        count_label=row_count_label(model),
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
        bounds_a=bounds(period_a_filters), bounds_b=bounds(period_b_filters),
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
        {row_count_expr} as period_{count_label}{extra_select}
    from {source_table} m
    {joins}
    {where_clause}
    group by date_trunc('{granularity}', m.{time_column}){group_by_extra}
)
select
    period,
    {group_by_select_out}period_value,
    period_{count_label},{extra_out}
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
                    group_by: list = None, close_periods: bool = True,
                    outputs: list = None) -> str:
    """
    Aylıq sıra + istəyə bağlı ƏLAVƏ ÇIXIŞLAR.

    'outputs' breakdown-dakı ilə EYNİ mexanizmdir (build_output_sql) — həmin
    kind-lər, həmin yoxlamalar. Trendə ona görə lazımdır ki, ekspertin bəzi
    sualları eyni aylıq sətirdə həm həcm, həm də ÇƏKİLİ ORTA faiz istəyir.

    ÖLÇÜLÜB (17.09.2026, qəbul sualı 13): ekspertin öz SQL-i məhz budur —
    ay + məhsul üzrə prolonged_contracts, total_prolonged_lcy VƏ
    weighted_avg_interest_rate. Bizim trend unit-i çəkili ortanı VERƏ
    BİLMİRDİ, uzadilma_icmali isə aylıq deyil; nəticədə sual üç işlətmədən
    üçündə dinamik SQL-ə düşdü. Şablona bir slot əlavə etmək yeni SQL yazmaq
    deyil — mövcud çıxış mexanizmini trendə də açmaqdır.

    Pəncərə funksiyaları (share_of_total) BURADA İŞLƏMİR: 'periods' CTE-si
    onsuz da qruplaşdırılıb, ikinci dəfə pəncərə açmaq mənasızdır.
    """
    agg_func = get_default_agg(model, measure_column)

    filters = apply_snapshot_default_filter(filters, model)
    conditions, joins = build_where_sql(filters, model, models, relationships, close_periods=close_periods)

    # Yarımçıq cari dövr DEFAULT olaraq kənarlaşdırılır — şablonun özündə,
    # modelin filtrindən asılı olmayaraq. Yalnız istifadəçi açıq şəkildə
    # "bu ay indiyə qədər" deyəndə daxil edilir.
    if not include_current_period:
        # close_month_periods() "son N ay" filtrinə eyni həddi onsuz da əlavə
        # edir — iki dəfə yazmaq SQL-i lüzumsuz uzadır.
        guard = current_period_guard(f"m.{_q(real_column_name(time_column, model))}",
                                     granularity)
        if guard not in conditions:
            conditions = conditions + [guard]

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

    # Əlavə çıxışlar 'periods' CTE-sində hesablanır və olduğu kimi ötürülür.
    extra_select, extra_out = "", ""
    for spec in outputs or []:
        kind = spec.get("kind", "sum")
        if kind == "share_of_total":
            raise ValueError(
                "time_series çıxışında 'share_of_total' işlədilə bilməz — "
                "sətirlər onsuz da dövr üzrə qruplaşdırılıb")
        extra_select += ",\n        " + build_output_sql(model, spec, group_by_parts)
        extra_out += "\n    " + _safe_label(spec["name"]) + ","

    return TIME_SERIES_TEMPLATE.format(
        extra_select=extra_select,
        extra_out=extra_out,
        count_label=row_count_label(model),
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


SNAPSHOT_TIME_SERIES_TEMPLATE = """with snapshot_dates as (
    select
        date_trunc('{granularity}', s.{time_column}) as period,
        max(s.{time_column}) as snapshot_date
    from {source_table} s
    {snapshot_where}
    group by date_trunc('{granularity}', s.{time_column})
),
periods as (
    select
        d.period,
        d.snapshot_date,
        {group_by_select}{agg_func}(m.{measure_column}) as period_value,
        {row_count_expr} as period_{count_label}{extra_select}
    from {source_table} m
    join snapshot_dates d on m.{time_column} = d.snapshot_date
    {joins}
    {where_clause}
    group by d.period, d.snapshot_date{group_by_extra}
)
select
    period,
    snapshot_date,
    {group_by_select_out}period_value,
    period_{count_label},{extra_out}
    lag(period_value) over ({partition_clause}order by period) as prior_period_value,
    round(
        (100.0 * (period_value - lag(period_value) over ({partition_clause}order by period))
        / nullif(lag(period_value) over ({partition_clause}order by period), 0))::numeric,
        2
    ) as {metric_name}
from periods
order by period desc{order_extra}"""


def op_snapshot_time_series(model: dict, models: dict, relationships: list,
                            measure_column: str, filters: list, time_column: str,
                            granularity: str, metric_name: str,
                            include_current_period: bool = False,
                            group_by: list = None, close_periods: bool = True,
                            outputs: list = None) -> str:
    """Stock cədvəli üçün hər dövrün son snapshot-ını seçən təhlükəsiz trend."""
    snapshot_name = model.get("snapshot_date_column")
    if not snapshot_name:
        raise ValueError("snapshot_time_series yalnız snapshot modelində işləyir")
    snapshot_column = real_column_name(snapshot_name, model)
    time_column = real_column_name(time_column, model)
    if not snapshot_column or snapshot_column != time_column:
        raise ValueError("snapshot_time_series modelin snapshot tarix sütununu tələb edir")
    if granularity not in ("day", "month", "quarter", "year"):
        raise ValueError(f"Dəstəklənməyən trend granularity-si: {granularity}")

    # Tarix seçimi yalnız zaman şərtlərinə baxır. Segmentin həmin gün sətri
    # yoxdursa köhnə günü seçib fərqli segmentləri müxtəlif tarixlərdən
    # müqayisə etmək olmaz; snapshot tarixi bütün cədvəl üçün vahiddir.
    time_filters = [f for f in filters or []
                    if _same_column(f.get("column"), time_column)]
    snapshot_conditions, snapshot_joins = build_where_sql(
        time_filters, model, models, relationships, alias="s",
        close_periods=close_periods)
    if snapshot_joins:
        raise ValueError("snapshot tarixinin seçimi başqa cədvələ qoşula bilməz")

    conditions, joins = build_where_sql(
        filters, model, models, relationships, alias="m",
        close_periods=close_periods)
    if not include_current_period:
        snapshot_guard = current_period_guard(f"s.{_q(time_column)}", granularity)
        outer_guard = current_period_guard(f"m.{_q(time_column)}", granularity)
        if snapshot_guard not in snapshot_conditions:
            snapshot_conditions.append(snapshot_guard)
        if outer_guard not in conditions:
            conditions.append(outer_guard)

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
        group_by_select = group_by_select_out = group_by_extra = ""
        partition_clause = order_extra = ""

    extra_select, extra_out = "", ""
    for spec in outputs or []:
        if spec.get("kind") == "share_of_total":
            raise ValueError("snapshot_time_series çıxışında share_of_total işlədilə bilməz")
        extra_select += ",\n        " + build_output_sql(model, spec, group_by_parts)
        extra_out += "\n    " + _safe_label(spec["name"]) + ","

    agg_func = get_default_agg(model, measure_column)
    return SNAPSHOT_TIME_SERIES_TEMPLATE.format(
        granularity=granularity,
        time_column=_q(time_column),
        source_table=_tbl(model),
        snapshot_where=("where " + " and ".join(snapshot_conditions))
        if snapshot_conditions else "",
        group_by_select=group_by_select,
        group_by_select_out=group_by_select_out,
        group_by_extra=group_by_extra,
        partition_clause=partition_clause,
        order_extra=order_extra,
        agg_func=agg_func,
        measure_column=_q(measure_column),
        row_count_expr=row_count_expr(model),
        count_label=row_count_label(model),
        extra_select=extra_select,
        extra_out=extra_out,
        joins="\n".join(joins),
        where_clause=("where " + " and ".join(conditions)) if conditions else "",
        metric_name=_q(metric_name),
    )


def op_ranked_breakdown(model: dict, models: dict, relationships: list,
                        outputs: list, filters: list, group_by: list,
                        ranking: dict, direction=None,
                        time_column=None, granularity: str = "month",
                        measure_column=None,
                        include_current_period: bool = False,
                        close_periods: bool = True) -> str:
    """Top/bottom N; dövrlü halda rank hər period daxilində ayrıca hesablanır."""
    direction = str(direction or ranking.get("direction", "desc")).lower()
    if direction not in ("asc", "desc"):
        raise ValueError("rank_direction yalnız 'asc' və ya 'desc' ola bilər")
    limit = ranking.get("limit", 1)
    if not isinstance(limit, int) or not 1 <= limit <= 100:
        raise ValueError("ranking.limit 1-100 aralığında tam ədəd olmalıdır")

    output_names = {_safe_label(spec["name"]) for spec in outputs or []}
    if time_column:
        if not measure_column:
            raise ValueError("Dövrlü ranking measure tələb edir")
        target = ranking.get("by", "period_value")
        allowed = output_names | {"period_value", f"period_{row_count_label(model)}"}
        if target not in allowed:
            raise ValueError(f"Ranking çıxışı '{target}' mövcud deyil: {sorted(allowed)}")
        operation = (op_snapshot_time_series if model.get("snapshot_date_column")
                     else op_time_series)
        base = operation(
            model, models, relationships,
            measure_column=measure_column,
            filters=filters,
            time_column=time_column,
            granularity=granularity,
            metric_name="__ranked_change_pct",
            include_current_period=include_current_period,
            group_by=group_by,
            close_periods=close_periods,
            outputs=outputs,
        )
        partition = "partition by period "
        tie_order = ", ".join(_q(c) for c in group_by or [])
        final_order = 'period desc, "__rank"' + (", " + tie_order if tie_order else "")
    else:
        target = ranking.get("by")
        allowed = output_names | {str(c) for c in group_by or []}
        if not target or target not in allowed:
            raise ValueError(f"Ranking çıxışı '{target}' mövcud deyil: {sorted(allowed)}")
        base = op_breakdown(
            model, models, relationships,
            outputs=outputs,
            filters=filters,
            group_by=group_by,
            temporal_scope=ranking.get("temporal_scope", "latest_snapshot"),
            close_periods=close_periods,
        )
        partitions = ranking.get("partition_by") or []
        unknown = {str(c) for c in partitions} - {str(c) for c in group_by or []}
        if unknown:
            raise ValueError(f"Ranking partition_by group_by-da yoxdur: {sorted(unknown)}")
        partition = ("partition by " + ", ".join(_q(c) for c in partitions) + " "
                     if partitions else "")
        tie_order = ", ".join(_q(c) for c in group_by or [])
        final_order = '"__rank"' + (", " + tie_order if tie_order else "")

    # Greenplum QUALIFY dəstəkləmir. Rank ayrıca CTE-də hesablanır, filtr isə
    # növbəti SELECT-də tətbiq olunur; pilot logundakı sintaksis xətası bu
    # ümumi şablonla aradan qalxır. Eyni ölçülü nəticələr dense_rank ilə birgə
    # saxlanır və group_by açarları ilə sabit sırada göstərilir.
    return f"""with base_values as (
{base}
),
ranked_values as (
    select
        base_values.*,
        dense_rank() over ({partition}order by {_q(target)} {direction} nulls last) as "__rank"
    from base_values
)
select *
from ranked_values
where "__rank" <= {limit}
order by {final_order}"""


# ---------------------------------------------------------------------------
# OVERLAP əməliyyatı bu PoC-də YOXDUR.
#
# "Neçə saving müştərisi eyni zamanda deposit müştərisidir" sualı iki müştəri
# çoxluğunun kəsişməsini tələb edir — bunun üçün unikal müştəri açarı (cif)
# lazımdır. dataiku_ai_coe aqreqat cədvəllərində belə açar YOXDUR: cif_countd
# və cif_distinct sütunları saydır, identifikator deyil. Onları kəsişdirmək
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
#   conditional_sum/share — etibarlı şərtə düşən ölçünün cəmi və ümumidə payı
#   conditional_ratio — ayrı şərtli iki cəmin nisbəti
# ---------------------------------------------------------------------------

BREAKDOWN_TEMPLATE = """select
    {select_list}
from {source_table} m
{joins}
{where_clause}
{group_by_clause}
{order_by_clause}"""

ALLOWED_OUTPUT_KINDS = (
    "sum", "weighted_avg", "share_of_total", "ratio", "sum_where",
    "conditional_sum", "conditional_share", "conditional_ratio",
)


def _measure_sql(model: dict, column: str, alias: str = "m") -> str:
    """
    Ölçü sütununa istinad — modeldə olmalıdır.

    Ad MODELDƏKİ registrə salınır: units.yml əl ilə yazılır, model isə canlı
    Greenplum-dan yaradılır (make models). Datamartda registr dəyişəndə
    (07.09.2026 qismən kiçik hərfə keçid) yeganə həqiqət mənbəyi modeldir —
    units.yml-in registri SQL-ə sızmamalıdır.
    """
    column = real_column_name(column, model)
    _, measures = _get_dims_and_measures(model)
    if column not in measures:
        raise ValueError(f"'{column}' modeldə ('{model['name']}') measure kimi tapılmadı")
    return f"{alias}.{_q(column)}"


def _dimension_sql(model: dict, column: str, alias: str = "m") -> str:
    column = real_column_name(column, model)
    dims, _ = _get_dims_and_measures(model)
    if column not in dims:
        raise ValueError(f"'{column}' modeldə ('{model['name']}') ölçü (dimension) deyil")
    if dims[column].get("unusable"):
        raise ValueError(f"'{column}' sütunu hazırda tam boşdur (NULL)")
    return f"{alias}.{_q(column)}"


def _condition_sql(model: dict, condition: dict, alias: str = "m") -> str:
    """Trusted-unit output condition; unlike WHERE filters it may cite a measure."""
    if not isinstance(condition, dict) or not condition.get("column"):
        raise ValueError("Şərt üçün 'column' tələb olunur")
    column = real_column_name(str(condition["column"]), model)
    dims, measures = _get_dims_and_measures(model)
    operator = str(condition.get("operator", "=")).upper()
    if operator not in ("=", "!=", ">", "<", ">=", "<=", "IN"):
        raise ParameterValidationError(f"Çıxış şərtində icazə verilməyən operator: {operator}")
    value = condition.get("value")

    if column in dims:
        column, operator, value = validate_filter(
            {"column": column, "operator": operator, "value": value}, model)
    elif column in measures:
        if not measures[column].get("conditional_filter"):
            raise ValueError(
                f"Measure şərti '{column}' conditional output üçün domen "
                "metadata-sında təsdiqlənməyib")
        values = value if isinstance(value, list) else [value]
        if any(not _is_numeric(v) for v in values):
            raise ParameterValidationError(
                f"Measure şərtinin dəyəri rəqəm olmalıdır: {value!r}")
        value = ([canonical_number(v) for v in value]
                 if isinstance(value, list) else canonical_number(value))
    else:
        raise ParameterValidationError(f"Şərt sütunu '{column}' modeldə tapılmadı")

    col_sql = f"{alias}.{_q(column)}"
    if operator == "IN":
        if not isinstance(value, list) or not value:
            raise ParameterValidationError(
                "Çıxış şərtində IN üçün boş olmayan siyahı tələb olunur")
        return f"{col_sql} in ({', '.join(_lit(v) for v in value)})"
    return f"{col_sql} {operator} {_lit(value)}"


def _output_conditions_sql(model: dict, spec: dict, alias: str = "m") -> str:
    conditions = list(spec.get("conditions") or [])
    if spec.get("condition"):
        conditions.append(spec["condition"])
    if not conditions:
        raise ValueError(f"'{spec.get('name')}': conditional output üçün şərt yoxdur")
    return " and ".join(
        _condition_sql(model, item, alias)
        for item in conditions)


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
        expr = f"{numerator} / nullif({denominator}, 0)"

    elif kind in ("conditional_sum", "conditional_share"):
        measure = _measure_sql(model, spec["measure"])
        condition = _output_conditions_sql(model, spec)
        inner = f"abs({measure})" if spec.get("absolute") else measure
        numerator = f"sum(case when {condition} then {inner} else 0 end)"
        expr = (numerator if kind == "conditional_sum" else
                f"100.0 * {numerator} / nullif(sum({measure}), 0)")

    elif kind == "conditional_ratio":
        numerator_spec = spec.get("numerator") or {}
        denominator_spec = spec.get("denominator") or {}
        for label, part in (("numerator", numerator_spec),
                            ("denominator", denominator_spec)):
            if not isinstance(part, dict) or not part.get("measure"):
                raise ValueError(f"'{name}': conditional_ratio {label} measure tələb edir")
            if not (part.get("condition") or part.get("conditions")):
                raise ValueError(f"'{name}': conditional_ratio {label} şərt tələb edir")

        def conditional_sum(part):
            measure = _measure_sql(model, part["measure"])
            condition = _output_conditions_sql(model, part)
            inner = f"abs({measure})" if part.get("absolute") else measure
            return f"sum(case when {condition} then {inner} else 0 end)"

        numerator = conditional_sum(numerator_spec)
        denominator = conditional_sum(denominator_spec)
        multiplier = 100.0 if spec.get("percent") else 1.0
        expr = f"{multiplier} * {numerator} / nullif({denominator}, 0)"

    else:  # sum_where
        measure = _measure_sql(model, spec["measure"])
        column = _dimension_sql(model, spec["column"])
        dims, _ = _get_dims_and_measures(model)
        allowed = dims[real_column_name(spec["column"], model)].get("allowed_values")
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
                 temporal_scope: str = "latest_snapshot", close_periods: bool = True) -> str:
    if not outputs:
        raise ValueError("breakdown üçün 'outputs' boş ola bilməz")

    filters = apply_snapshot_default_filter(filters, model, temporal_scope)
    conditions, joins = build_where_sql(filters, model, models, relationships, close_periods=close_periods)

    group_by_parts, select_dims = [], []
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
    # Hər iki tərəf MODELDƏKİ ada salınır: group_by compile_query-də artıq
    # normallaşdırılıb, partition_by isə units.yml-dən olduğu kimi gəlir.
    group_by_set = {real_column_name(c, model) for c in group_by or []}
    for spec in outputs:
        if spec.get("kind") == "share_of_total":
            for column in (real_column_name(c, model)
                           for c in spec.get("partition_by") or []):
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
    Unit həqiqətən non-additive ölçü (cif_countd və s.) istifadə edirmi?

    Yalnız 'allow_approximate' bayrağına baxmaq YANLIŞ olardı — bəzi unit-lər
    bayrağı daşıyır, amma çıxışlarında müştəri sayı yoxdur. Onda istifadəçiyə
    yersiz xəbərdarlıq göstərilərdi.
    """
    _, measures = _get_dims_and_measures(model)
    # REGİSTRDƏN ASILI OLMAYARAQ. units.yml əl ilə yazılır ('cif_distinct'),
    # model isə canlı Greenplum-dan gəlir və satış cədvəli qarışıq registrdədir
    # ('CIF_distinct'). Sadə çoxluq kəsişməsi bunu görmürdü: satis_strukturu,
    # kanal_miksi_satis və uzadilma_icmali müştəri sayını CƏMLƏYİR, amma
    # "təxmini" xəbərdarlığı HEÇ VAXT qalxmırdı (ölçülüb).
    non_additive = {name.lower() for name, spec in measures.items()
                    if spec.get("additive") is False}
    if not non_additive:
        return False

    used = set()
    based_on_measure = (unit.get("based_on") or {}).get("measure")
    if based_on_measure:
        used.add(str(based_on_measure).lower())
    for spec in unit.get("outputs") or []:
        for key in ("measure", "numerator", "denominator", "weight"):
            if spec.get(key):
                used.add(str(spec[key]).lower())
    return bool(used & non_additive)


def _bind_output_conditions(outputs: list, semantic_query: dict) -> list:
    """Unit output-un elan etdiyi condition slotunu validated runtime dəyərinə bağla."""
    bound = []
    for original in outputs or []:
        spec = dict(original)
        slot = spec.pop("condition_slot", None)
        if slot:
            spec["conditions"] = list(semantic_query.get(slot) or [])
        bound.append(spec)
    return bound


def compile_query(semantic_query: dict) -> str:
    models = load_models()
    metrics = load_metrics()
    relationships = load_relationships()

    metric_name = semantic_query["metric"]
    metric_def = metrics[metric_name]

    # ADR-0005: model YALNIZ icazə verilən yuvalara dəyər qoya bilər. Elan
    # edilməyən yuva SƏSSİZCƏ atılırdı — ölçülüb: Infer Agent 'satis_hecmi'
    # (aggregate) unit-inə "period_b_filters" göndərdi, slot nəzərə alınmadı və
    # sorğu "son 6 ay" ilə eyni oldu; iki fərqli alt-sual təkrar sayılıb
    # birləşdirildi və "əvvəlki 6 ay" rəqəmi heç vaxt hesablanmadı.
    # "bu ay indiyə qədər" istəyi YALNIZ trendə deyil, hər əməliyyata aiddir:
    # o olmadan "son N ay" = son N TAM ay (close_month_periods).
    close_periods = not bool(semantic_query.get("include_current_period", False))

    declared = set((metric_def.get("parameters") or {}).keys())
    for slot in ("filters", "scope_filters", "subgroup_filters", "conditions",
                 "period_a_filters", "period_b_filters", "group_by"):
        if semantic_query.get(slot) and slot not in declared:
            raise ParameterValidationError(
                f"'{slot}' bu unit üçün elan edilməyib ({metric_name}). "
                f"İcazə verilən parametrlər: {', '.join(sorted(declared)) or 'yoxdur'}"
            )
    model = models[metric_def["based_on"]["model"]]
    semantic_query = domain_policy.enforce(semantic_query, metric_def, model)
    measure_column = metric_def["based_on"].get("measure")
    # units.yml əl ilə yazılır, model isə canlı Greenplum-dan gəlir —
    # registr fərqi olarsa MODELDƏKİ ad üstündür.
    if measure_column:
        measure_column = real_column_name(measure_column, model)
    op_type = metric_def["operation"]
    outputs = _bind_output_conditions(metric_def.get("outputs", []), semantic_query)
    if semantic_query.get("rank_direction") and op_type != "ranked_breakdown":
        raise ValueError("rank_direction yalnız ranked_breakdown unit-ində işlədilə bilər")

    user_filters = semantic_query.get("filters", [])
    group_by = [real_column_name(c, model)
                for c in semantic_query.get("group_by") or []]
    _check_required_group_by(group_by, metric_def, model)
    _check_required_filters(semantic_query, metric_def, model)
    check_snapshot_range(semantic_query, metric_def, model)

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
            close_periods=close_periods,
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
            close_periods=close_periods,
        )

    elif op_type == "share_case_when":
        temporal_scope = metric_def.get("temporal_scope", "latest_snapshot")
        return op_share_case_when(
            model, models, relationships,
            measure_column=measure_column,
            case_column=real_column_name(metric_def["case_column"], model),
            case_value_a=metric_def["case_value_a"],
            case_value_b=metric_def["case_value_b"],
            label_a=metric_def["label_a"],
            label_b=metric_def["label_b"],
            filters=user_filters,
            group_by=group_by,
            metric_name=metric_name,
            temporal_scope=temporal_scope,
            close_periods=close_periods,
        )

    elif op_type == "compare":
        # ÖLÇÜLÜB: burada yalnız unit-in öz sabit filtrləri və semantic_query-nin
        # "filters" slotu oxunurdu. Infer Agent isə dövrləri DÜZGÜN olaraq
        # period_a_filters / period_b_filters slotlarına yazır — onlar heç vaxt
        # oxunmadığı üçün HƏR İKİ dövr WHERE-siz qalırdı: iki sətir də bütün
        # tarixin cəmi olurdu. Nəticə eyni iki rəqəm idi, cavab isə "satışlar
        # 73.6% azalıb" kimi TAMAMİLƏ YANLIŞ çıxırdı.
        base_a = metric_def.get("period_a_filters", [])
        base_b = metric_def.get("period_b_filters", [])
        return op_compare(
            model, models, relationships,
            measure_column=measure_column,
            period_a_filters=base_a + user_filters + (semantic_query.get("period_a_filters") or []),
            period_b_filters=base_b + user_filters + (semantic_query.get("period_b_filters") or []),
            period_a_label=metric_def.get("period_a_label", "period_a"),
            period_b_label=metric_def.get("period_b_label", "period_b"),
            report_period_bounds=bool(semantic_query.get("report_period_bounds")),
            metric_name=metric_name,
            group_by=group_by,
            close_periods=close_periods,
        )

    elif op_type == "breakdown":
        base_filters = metric_def.get("base_filters", [])
        return op_breakdown(
            model, models, relationships,
            outputs=outputs,
            filters=base_filters + user_filters,
            group_by=group_by,
            temporal_scope=metric_def.get("temporal_scope", "latest_snapshot"),
            close_periods=close_periods,
        )

    elif op_type == "time_series":
        base_filters = metric_def.get("base_filters", [])
        # Yalnız istifadəçi açıq istəyəndə true olur (Infer Agent-dən gəlir).
        include_current = bool(semantic_query.get("include_current_period", False))
        return op_time_series(
            model, models, relationships,
            measure_column=measure_column,
            filters=base_filters + user_filters,
            time_column=real_column_name(metric_def["time_column"], model),
            granularity=metric_def.get("granularity", "month"),
            metric_name=metric_name,
            include_current_period=include_current,
            group_by=group_by,
            close_periods=close_periods,
            outputs=outputs,
        )

    elif op_type == "snapshot_time_series":
        base_filters = metric_def.get("base_filters", [])
        include_current = bool(semantic_query.get("include_current_period", False))
        return op_snapshot_time_series(
            model, models, relationships,
            measure_column=measure_column,
            filters=base_filters + user_filters,
            time_column=real_column_name(metric_def["time_column"], model),
            granularity=metric_def.get("granularity", "month"),
            metric_name=metric_name,
            include_current_period=include_current,
            group_by=group_by,
            close_periods=close_periods,
            outputs=outputs,
        )

    elif op_type == "ranked_breakdown":
        base_filters = metric_def.get("base_filters", [])
        return op_ranked_breakdown(
            model, models, relationships,
            outputs=outputs,
            filters=base_filters + user_filters,
            group_by=group_by,
            ranking=metric_def.get("ranking") or {},
            direction=semantic_query.get("rank_direction"),
            time_column=(real_column_name(metric_def["time_column"], model)
                         if metric_def.get("time_column") else None),
            granularity=metric_def.get("granularity", "month"),
            measure_column=measure_column,
            include_current_period=bool(semantic_query.get("include_current_period", False)),
            close_periods=close_periods,
        )

    else:
        raise ValueError(f"Naməlum əməliyyat tipi: {op_type}")
