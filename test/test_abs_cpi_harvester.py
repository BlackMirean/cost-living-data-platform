from backend.harvesters.abs_cpi_harvester import labelled_value_parts, parse_cpi_csv


def test_labelled_value_parts():
    assert labelled_value_parts("10001: All groups CPI") == ("10001", "All groups CPI")


def test_parse_cpi_csv():
    csv_text = """DATAFLOW,MEASURE: Measure,INDEX: Index,TSEST: Adjustment Type,REGION: Region,FREQ: Frequency,TIME_PERIOD,OBS_VALUE,UNIT_MEASURE: Unit of Measure
ABS:CPI(2.0.0),1: Index Numbers,10001: All groups CPI,10: Original,50: Australia,M: Monthly,2026-03,104.2,
"""
    docs = parse_cpi_csv(csv_text)
    assert docs[0]["indicator"] == "monthly_cpi"
    assert docs[0]["dataflow"] == "ABS:CPI(2.0.0)"
    assert docs[0]["measure_code"] == "1"
    assert docs[0]["item_name"] == "All groups CPI"
    assert docs[0]["adjustment"] == "Original"
    assert docs[0]["region_code"] == "50"
    assert docs[0]["region"] == "Australia"
    assert docs[0]["frequency_code"] == "M"
    assert docs[0]["period"] == "2026-03"
    assert docs[0]["value"] == 104.2
    assert docs[0]["raw_row"]["OBS_VALUE"] == "104.2"
