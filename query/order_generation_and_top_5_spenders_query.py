client = MongoClient('mongodb+srv://edwinciw:e13d97c46@amazone.4sbahbn.mongodb.net/')
result = client['amazone']['customers'].aggregate(
  [
    {
      $lookup: {
        from: 'past_orders',
        localField: 'customer_id',
        foreignField: 'customer_id',
        as: 'past',
        pipeline: [
          { $project: { total_amount: 1 } }
        ]
      }
    },
    { $unwind: '$past' },
    {
      $lookup: {
        from: 'current_orders',
        localField: 'customer_id',
        foreignField: 'customer_id',
        as: 'current',
        pipeline: [
          {
            $project: {
              status: 1,
              total_amount: 1
            }
          }
        ]
      }
    },
    { $unwind: '$current' },
    {
      $group: {
        _id: '$customer_id',
        total_paid_past: {
          $sum: '$past.total_amount'
        },
        total_paid_current: {
          $sum: {
            $cond: {
              if: {
                $and: [
                  {
                    $ne: [
                      '$current.status',
                      'Out For Delivery'
                    ]
                  },
                  {
                    $ne: [
                      '$current.status',
                      'In Cart'
                    ]
                  }
                ]
              },
              then: '$current.total_amount',
              else: 0
            }
          }
        }
      }
    },
    {
      $set: {
        customer_id: '$_id',
        total_spend: {
          $add: [
            '$total_paid_past',
            '$total_paid_current'
          ]
        }
      }
    },
    {
     $sort:{
         'total_spend':-1
         }
     },
    {
     $limit : 5
     },
    {
      $project: {
        _id: 0,
        customer_id: 1,
        total_spend: 1
      }
    }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);




