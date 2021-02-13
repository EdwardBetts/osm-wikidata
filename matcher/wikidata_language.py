from . import wikidata_api


def languages_from_entity(entity):
    languages = languages_from_country_entity(entity)
    if languages or "P17" not in entity["claims"]:
        return languages

    for c in entity["claims"]["P17"]:
        country_qid = c["mainsnak"]["datavalue"]["value"]["id"]
        country_entity = wikidata_api.get_entity(country_qid)
        languages = languages_from_country_entity(country_entity)
        if languages:
            return languages


def get_lang_qids(entity):
    if "P37" not in entity["claims"]:
        return []

    lang_qids = []
    for lang in entity["claims"]["P37"]:
        if "datavalue" not in lang["mainsnak"]:
            continue
        lang_qid = lang["mainsnak"]["datavalue"]["value"]["id"]
        if lang_qid in ("Q7850", "Q727694", "Q3110592"):  # Chinese
            lang_qids += ["Q7850", "Q13414913", "Q18130932"]
            continue
        if lang_qid == "Q18784":  # North Korean standard language
            lang_qids += ["Q9176"]
        if lang_qid == "Q33298":  # Filipino -> Tagalog
            lang_qids += ["Q34057"]
        lang_qids.append(lang_qid)

    return lang_qids


def languages_from_country_entity(entity):
    lang_qids = get_lang_qids(entity)

    if not lang_qids:
        return []

    entities = wikidata_api.get_entities(lang_qids)
    return process_language_entities(entities)


def process_language_entities(entities):
    languages = []
    for lang in entities:
        claims = lang["claims"]
        l = {
            "en": lang["labels"]["en"]["value"],
        }
        if "P424" not in claims:
            continue
        mainsnak = claims["P424"][0]["mainsnak"]
        if "datavalue" not in mainsnak:
            continue
        p424 = mainsnak["datavalue"]["value"]
        l["code"] = p424
        if p424 not in lang["labels"]:
            continue
        l["local"] = lang["labels"][p424]["value"]

        languages.append(l)

    return languages
