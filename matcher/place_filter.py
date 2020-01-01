from flask import request, g
from .model import IsA

def get_filter_items():
    filter_qids = request.args.getlist('filter')
    if len(filter_qids) == 1:
        filter_qids = filter_qids[0].split(';')

    if filter_qids:
        g.filter_qids = ';'.join(filter_qids)

    filter_items = [IsA.query.get(int(qid[1:])) for qid in filter_qids]

    return filter_items


