const { MongoClient, ObjectId } = require('mongodb');

const express = require('express');
const res = require('express/lib/response');
const app = express()
var bodyparser = require('body-parser')

app.use(bodyparser.json())


// Connection URL
const url = 'mongodb://0.0.0.0:27017';

// Database Name
const dbsample_mflix = 'sample_mflix';
const dbsample_analytics = 'sample_analytics';
const dbairbnb = 'airbnb';

(async ()=>{
  try{
  const client = new MongoClient(url);
  await client.connect();


  console.log('Connected successfully to server');

// Database mflix
  const dbMflix = client.db(dbsample_mflix);

  const dbMflixcollectionComments = dbMflix.collection('comments');
  const dbMflixcollectionMovies = dbMflix.collection('movies');
  const dbMflixcollectionTheaters = dbMflix.collection('theaters');
  const dbMflixcollectionUsers = dbMflix.collection('users');
 
// Database analytics

const dbanalytics = client.db(dbsample_analytics);

  const dbanalyticscollectionAccounts = dbanalytics.collection('accounts');
  const dbanalyticscollectionCustomers = dbanalytics.collection('customers');
  const dbanalyticscollectionTransactions = dbanalytics.collection('transactions');

// Database training

const dbAirbnb = client.db(dbairbnb);

  const dbtrainingcollectionReview = dbAirbnb.collection('review');
  

// Routes mflix

  app.get('/mflix/getMovies', async (req, res) => {

    const result = await dbMflixcollectionMovies.aggregate([
        {
          $lookup: {
            from: "comments",
            localField: "_id",
            foreignField: "movie_id",
            as: "comments"
          }
        },
        {
            $sort: { released: -1 }
          },
        {
          $project: {
            _id: 0,
            title: 1,
            released: 1,
            comments: {
                
                name: 1,
                email: 1,
                text: 1,
                date: 1
              },
          }
        },
        
      ]).limit(10).toArray()
    
      res.json(result)
    })

    
  app.post('/mflix/addMovie', async (req, res) => {
  
    const newMovie = req.body;
    await dbMflixcollectionMovies.insertOne(newMovie);
    res.send('Film ajouté');
   
  })


  app.get('/getMovieByName/:title', async (req, res) => {
    const movie = await dbMflixcollectionMovies.findOne({title: req.params.title})
    res.send(JSON.stringify(movie))
  })

    app.delete('/mflix/deleteMovie/:title', async (req, res) => {
    await dbMflixcollectionMovies.deleteOne({title: req.params.title})
    res.send('film suprimer')
  })


  app.put('/mflix/updateMovie/:title', async (req, res) => {
    const newMovie = req.body;
    await dbMflixcollectionMovies.updateOne({title: req.params.title}, {$set: newMovie })
    res.send('film modifier')
  })    

  
  app.get('/mflix/getRankedMoviesByCommentsNumber', async (req, res) => {
    
    const result = await dbMflixcollectionMovies.aggregate([
     
        {
            $sort: { num_mflix_comments: -1 }
          
        },
          {$project: {
            num_mflix_comments: 0
          },}

        
      ]).limit(10).toArray()
    
      res.json(result)
  })

  app.get('/mflix/getComments', async (req, res) => {
    
    const result = await dbMflixcollectionComments.aggregate([
        
        {
            $sort: { date: -1 }
          
        },  
        {
            $lookup: {
                from: "users",
                localField: "email",
                foreignField: "email",
                as: "user"
            }
        },
        {
            $lookup: {
                from: "movies",
                localField: "movie_id",
                foreignField: "_id",
                as: "movie"
        }
        },
        { $unwind: "$user" },
        { $unwind: "$movie" },

        {$project: {
                _id: 0,
                text: 1,
                date: 1,
            user: {
                name: 1,
                email: 1
                
              },
              movie: {
                plot: 1,
                genres: 1,
                cast: 1,
                title: 1
                
              }
          }}
     


        
      ]).limit(10).toArray()
    
      res.json(result)
  })

  app.get('/mflix/getComment/:comment', async (req, res) => {
    const comment = await dbMflixcollectionComments.find({
      text: {
        $regex: req.params.comment, // Utiliser $regex pour effectuer une recherche de texte partiel
        $options: 'i' // Utiliser $options pour effectuer une recherche insensible à la casse
      }
    }).toArray()
    res.send(JSON.stringify(comment))
  })
  
  
  app.post('/mflix/addComment/', async (req, res) => {
    return dbMflixcollectionComments.insertOne(req.body),
    res.send('Commentaire ajouté')
  })

  app.put('/mflix/updateComment/:_id', async (req, res) => {
    const newComment = req.body;
    await dbMflixcollectionComments.updateOne({_id: new ObjectId(req.params._id)}, {$set: newComment })
    res.send('Commentaire modifier')
  })

  app.delete('/mflix/deleteComment/:_id', async (req, res) => {
    await dbMflixcollectionComments.deleteOne({_id: new ObjectId(req.params._id)})
    res.send('Commentaire suprimer')
  })

  

  app.get('/mflix/getUsers', async (req, res) => {
    
    const result = await dbMflixcollectionUsers.aggregate([
         
        {
            $lookup: {
                from: "comments",
                localField: "email",
                foreignField: "email",
                as: "comment"
            }
        },
        
          
        {
            $sort: { date: -1 }
          
        },
        
        {$project: {
            comment: 0
      }}
 
    
      ]).limit(20).toArray()
    
      res.json(result)
  })


  app.delete('/mflix/deleteUser/:email', async (req, res) => {
    await dbMflixcollectionUsers.deleteOne({email: req.params.email})
    res.send('Users suprimer')
  })

  app.put('/mflix/updateUser/:_id', async (req, res) => {
    const newUser = req.body;
    await dbMflixcollectionUsers.updateOne({_id: new ObjectId(req.params._id)}, {$set: newUser })
    res.send('user modifier')
  })

  app.get('/mflix/getUserByEmail/:email', async (req, res) => {
    const result = await dbMflixcollectionUsers.findOne({email: req.params.email})
    res.json(result)
  })
  

  
// Routes analytics


    app.post('/analytics/createAccountNumber', async (req, res) => {
        const result = await dbanalyticscollectionCustomers.aggregate([
         
          
            {$project: {
                username: 1,
                name: 1,
                addresse: 1,
                email: 1,
                nbrAccounts: {$size : "$accounts"}
          }},

          {
            $out: { db: "sample_analytics", coll: "countedAccount"}
          },
     
        
          ]).toArray()
          res.send('Collection créé')
    })


    app.get('/analytics/countedAccount', async (req, res) => {
        const countedAccount = dbanalytics.collection('countedAccount');
        const result = await countedAccount.find().toArray();
        res.json(result)
      })

      app.post('/analytics/transactionsByDay', async (req, res) => {
        await dbanalyticscollectionTransactions.aggregate([
          { $project: {
              _id: 0,
              date: 1,
              transactions: 1
            }
          },
          { $unwind: "$transactions" },
          { $group: {
              _id: "$transactions.date",
              transactions: { $push: "$transactions" },
              totalAmount: { $sum: "$transactions.amount" }
            }
          },
          {
            $out: { db: "sample_analytics", coll: "TransactionPerDay"}
          },
     
        
          ]).toArray()
          res.send('Collection créé')
      });

      
    app.get('/analytics/transactionsByDay', async (req, res) => {
        const transactionsByDay = dbanalytics.collection('TransactionPerDay');
        const result = await transactionsByDay.find().toArray();
        res.json(result)
      })

      
      
      app.post('/analytics/createRankedTransaction', async (req, res) => {
       const result = await dbanalyticscollectionTransactions.aggregate([
        
          {
            $lookup: {
              from: "customers",
              localField: "account_id",
              foreignField: "accounts",
              as: "customer"
            }
          }, { $unwind: "$customer" },
          {
            $project: {
              transactions: 0,
            }
          },
          { $group: {
            _id: "$customer._id",
            name: {$first : "$customer.name"},
            username: {$first : "$customer.username"},
            totalTransactions: { $sum: "$transaction_count" }
          }
        },
        {$sort: {totalTransactions: -1}},
        {
            $out: { db: "sample_analytics", coll: "RankedTransaction"}
          },
        ]).toArray();
        res.send('Collection créé')
      });

      app.get('/analytics/createRankedTransaction', async (req, res) => {
        const createRankedTransaction = dbanalytics.collection('RankedTransaction');
        const result = await createRankedTransaction.find().toArray();
        res.json(result)
      })


      
// Routes airbnb
      
    // trouver les pays les mieux noter en pourcentage sur le total de leurs logement loué

      app.get('/airbnb/average/review', async (req, res) => {
        const result = await dbtrainingcollectionReview.aggregate([
          {
            $project: {
              review_scores: { review_scores_rating: 1 },
              address: { country: 1 },
            },
          },
          {
            $group: {
              _id: "$address.country",
              totalReviews: { $sum: "$review_scores.review_scores_rating" },
                count: { $sum: 1 }
            },
            
          },
          {
            $addFields: {
                averagerate: {$divide: ["$totalReviews", "$count"] },
            }
          },
          {$sort: {averagerate: -1}},
          
        ]).toArray();
        res.json(result)
      })
      
// sort les logement ou il y a le plus de lits par chambre

      app.get('/airbnb/bed', async (req, res) => {
            const result = await dbtrainingcollectionReview.aggregate([
                {
                    $match:{
                        bedrooms: {$ne: 0}
                    }
                },
              {
                $project: {
                  bedrooms:  1 ,
                  beds: 1,
                  address: { country: 1 }
                },
              },
              {
                $addFields: {
                    nbBedInBedrooms: {$divide: ["$beds","$bedrooms"] },
                }
              },
              {$sort: { nbBedInBedrooms: -1}},
              
         
              
            ]).toArray();
        res.json(result)
      })
 




  


  
 
  
  

//   app.get('/', async (req, res) => {
//     const result = await collectionUsers.findOne()
//     res.send(JSON.stringify(result))
//   })


//   app.get('/countUserComments',async (req,res) => {
//    const result = await collectionComments.aggregate([
//       {
//         $lookup: {
//           from: "users",
//           localField: "email",
//           foreignField: "email",
//           as: "user"
//         }
//       },
//       { $unwind: "$user" },
//       {$group : {_id:"$user._id", email: {$first : "$user.email"}, count:{$sum:1}}},


//       {$sort :  { count : -1 } },
//       {
//         $out: { db: "sample_mflix", coll: "countedComments"}
//       },
//     ]).toArray()
//     res.send(JSON.stringify(result))
//   })

//   app.get('/countUserCommentsByMovies',async (req,res) => {
//     const result = await collectionComments.aggregate([
//        {
//          $lookup: {
//            from: "users",
//            localField: "email",
//            foreignField: "email",
//            as: "user"
//          }
//        },
//        {
//         $lookup: {
//           from: "movies",
//           localField: "movie_id",
//           foreignField: "_id",
//           as: "movie"
//         }
//       },
//        { $unwind: "$user" },
//        { $unwind: "$movie" },
//        { $unwind: "$movie.genres" },
       
      
//        {$group:{ _id:"$user._id", email: {$first : "$user.email"}, genres :{$addToSet:"$movie.genres"}}},
//        {$project: { _id: 1, email: 1, genresCount: {$size:"$genres"}}},

//        {$sort :  { genresCount : -1 } },
//      ]).limit(10).toArray()
//      res.json(result)
//    })


  app.listen(3000, () => console.log('listening to port'))


  } catch(e){
    console.log(e);
  }


  
})()

