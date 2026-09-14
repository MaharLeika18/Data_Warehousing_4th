db.sales_documents.aggregate([
    {
        "$match": {
            "status": "completed"
        }
    },
    {
        "$group": {
            "_id": "$product_category",
            "total_units_sold": {
                "$sum": "$quantity"
            },
            "total_revenue": {
                "$sum": {
                    "$multiply": ["$quantity", "$unit_price"]
                }
            }
        }
    },
    {
        "$sort": {
            "total_revenue": -1
        }
    },
    {
        "$limit": 3
    }
])