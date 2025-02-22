
def create_update_aggregation(resource_name, field_name, calc_tree, match_agg, updater_id=None):
    if calc_tree._is_lookup():
        calc_agg = calc_tree.create_aggregation()
        agg = match_agg + [
            {"$lookup": {
                "from": "resource_%s" % resource_name,
                "as": field_name,
                "let": {"id": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$_id", "$$id"]}}},
                ] + calc_agg
            }},
        ]
        if calc_tree.is_collection():
            if calc_tree.is_primitive():
                agg.extend([
                    {"$addFields": {
                        field_name: "$%s._val" % field_name,
                    }}
                ])
        else:
            # for links
            agg.extend([
                {"$addFields": {
                    field_name: {"$arrayElemAt": ["$%s" % field_name, 0]},
                }}])
            if calc_tree.is_primitive():
                agg.extend([
                    {"$addFields": {
                        field_name: "$%s._val" % field_name,
                    }}
                ])
            else:
                agg.extend([
                    {"$addFields": {
                        field_name: "$%s._id" % field_name,
                    }}
                ])
    else:
        calc_agg = calc_tree.create_aggregation()
        agg = calc_agg + [
            {"$addFields": {field_name: "$_val"}},
        ]

    if calc_tree.is_collection():
        if calc_tree.is_primitive():
            agg.extend([
                {"$project": {field_name: 1}},
            ])
        else:
            agg.extend([
                {"$project": {"%s._id" % field_name: 1}},
            ])
    else:
        agg.extend([
            {"$project": {field_name: 1}},
        ])

    agg.extend([
        {"$merge": {
            "into": "resource_%s" % resource_name,
            "on": "_id",
        }}
    ])
    if updater_id:
        agg[-1]['$merge']["whenMatched"] = [
            {"$addFields": {
                "_dirty.%s" % updater_id: {"$cond": {
                    "if": {"$ne": ["$$new.%s" % field_name, "$%s" % field_name]},
                    "then": field_name,
                    "else": "$$REMOVE"
                }},
                field_name: "$$new.%s" % field_name
            }}
        ]


    return agg
