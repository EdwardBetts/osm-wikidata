from collections import Counter
from .model import IsA, Language

def get_isa_facets(items, languages=None, min_count=4):
    if languages is None:
        languages = [Language.get_by_code('en')]

    isa_counts = Counter()
    isa_labels = {}

    for item in items:
        for isa in item.isa:
            isa_counts[isa.qid] += 1
            if isa.qid not in isa_labels:
                isa_labels[isa.qid] = isa.label_and_description(languages)
            try:
                super_list = [claim['mainsnak']['datavalue']['value']['id']
                              for claim in isa.entity['claims'].get('P279', [])]
            except TypeError:
                super_list = []
            for super_isa in super_list:
                isa_counts[super_isa] += 1

    top_facets = []

    for qid, count in isa_counts.most_common():
        if min_count and count < min_count:
            continue
        if qid in isa_labels:
            label = isa_labels[qid]
        else:
            isa = IsA.query.get(qid[1:])
            if isa is None:
                continue
            label = isa.label_and_description(languages)
        top_facets.append({
            'count': count,
            'label': label['label'],
            'lang': label['lang'],
            'description': label['description'],
            'qid': qid,
        })

    return top_facets

def get_isa_facets2(items, languages=None, min_count=4):
    if languages is None:
        languages = [Language.get_by_code('en')]

    isa_counts = Counter()
    isa_labels = {}

    for item in items:
        for isa in item.isa:
            isa_counts[isa.qid] += 1
            if isa.qid not in isa_labels:
                isa_labels[isa.qid] = isa.label_and_description_list(languages)
            try:
                super_list = [claim['mainsnak']['datavalue']['value']['id']
                              for claim in isa.entity['claims'].get('P279', [])]
            except TypeError:
                super_list = []
            for super_isa in super_list:
                isa_counts[super_isa] += 1

    top_facets = []

    for qid, count in isa_counts.most_common():
        if min_count and count < min_count:
            continue
        if qid in isa_labels:
            label = isa_labels[qid]
        else:
            isa = IsA.query.get(qid[1:])
            if isa is None:
                continue
            label = isa.label_and_description_list(languages)
        top_facets.append({
            'count': count,
            'labels': label,
            'qid': qid,
        })

    return top_facets
