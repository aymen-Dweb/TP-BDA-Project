//1
db.HOTEL.find({ETOILES: 3})

//////////////////////////////////////////////


//2-1
db.RESERVATION.aggregate([
  {$lookup: {from: "HOTEL", localField: "NUMHOTEL", foreignField: "NUMHOTEL", as: "HotelDetails"}},
  {$unwind: "$HotelDetails"},
  {$group: {_id: "$HotelDetails.NOMHOTEL", totalReservations: {$sum: 1}}},
  {$sort: {totalReservations: -1}},
  {$out: "HotelsNbResv"}
])
//2-2
db.HotelsNbResv.find()

//////////////////////////////////////////////


//3
db.CHAMBRE.aggregate([
  {$lookup: {from: "HOTEL", localField: "NUMHOTEL", foreignField: "NUMHOTEL", as: "HotelDetails"}},
  {$unwind: "$HotelDetails"},
  {$match: {PRIXNUIT: {$lte: 6000}}},
  {$project: {_id: 0, NOMHOTEL: "$HotelDetails.NOMHOTEL"}}
])
//////////////////////////////////////////////

//4
db.EVALUATION.aggregate([
  {$group: {_id: "$NUMHOTEL", moyenneNote: {$avg: "$Note"}}},
  {$match: {moyenneNote: {$gte: 5}}},
  {$lookup: {from: "HOTEL", localField: "_id", foreignField: "NUMHOTEL", as: "HotelDetails"}},
  {$unwind: "$HotelDetails"},
  {$project: {_id: 0, NOMHOTEL: "$HotelDetails.NOMHOTEL"}}
])

//////////////////////////////////////////////


//5
db.RESERVATION.aggregate([
  {$match: {"CLIENT.Email": "client@mail.com"}},
  {$lookup: {from: "HOTEL", localField: "NUMHOTEL", foreignField: "NUMHOTEL", as: "HotelDetails"}},
  {$unwind: "$HotelDetails"},
  {$lookup: {from: "CHAMBRE", localField: "NUMCHAMBRE", foreignField: "NUMCHAMBRE", as: "ChambreDetails"}},
  {$unwind: "$ChambreDetails"},
  {$project: {_id: 0, NOMHOTEL: "$HotelDetails.NOMHOTEL", NUMCHAMBRE: "$ChambreDetails.NUMCHAMBRE", DATEARRIVEE: 1}}
])

//////////////////////////////////////////////



//6
db.EVALUATION.aggregate([
  {$lookup: { from: "CLIENT",localField: "NUMCLIENT",foreignField: "NUMCLIENT",as: "client"}},
  {$unwind: "$client"},
  {$match: {"client.Email": "email_du_client"}},
  {$project: { _id: 0, NOMHOTEL: 1, DATE: 1, Note: 1}}
])

//////////////////////////////////////////////


//7
var hotels = d.HOTELS.distinct("NUMHOTEL",{ETOILES:5})
db.CHAMBRE.updateMany(
  { NUMHOTEL : {$in:hotels}
  },
  { $inc: { PRIXNUIT: 2000 } }
)

//////////////////////////////////////////////


//8-1
var mapFunction = function() {
  emit(this.NUMHOTEL, 1);
};

var reduceFunction = function(key, values) {
  return Array.sum(values);
};

var finalizeFunction = function(key, reducedValue) {
  return { hotel: key, nb_reservations: reducedValue };
};

db.RESERVATION.mapReduce(
  mapFunction,
  reduceFunction,
  {
    out: "Hotels_NbResv",
    finalize: finalizeFunction
  }
);
//8-2
db.Hotels_NbResv.find({},{"_id":0}).sort({ nb_reservations: -1 });