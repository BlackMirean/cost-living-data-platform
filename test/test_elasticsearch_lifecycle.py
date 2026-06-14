from scripts import apply_elasticsearch_lifecycle as lifecycle


def test_processed_template_configures_ilm_and_alias():
    template = lifecycle.processed_template("cost_living_processed_posts_write")

    assert template["index_patterns"] == ["cost_living_processed_posts-*"]
    settings = template["template"]["settings"]
    assert settings["index.lifecycle.name"] == lifecycle.POLICY_NAME
    assert settings["index.lifecycle.rollover_alias"] == "cost_living_processed_posts_write"
    assert "raw_id" in template["template"]["mappings"]["properties"]
