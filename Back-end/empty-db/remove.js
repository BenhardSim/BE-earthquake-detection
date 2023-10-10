const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const admin = require('firebase-admin');

const serviceAccount = require('../../credentials.json'); // Replace with your service account credentials
const app = express();
const server = http.createServer(app);
const io = socketIo(server);
const cors = require('cors');

admin.initializeApp(
  {
    credential: admin.credential.cert(serviceAccount),
    databaseURL: 'https://eews-pipeline-default-rtdb.asia-southeast1.firebasedatabase.app/', // Replace with your Firebase project URL
  }
);


// Create a reference to your Firebase Realtime Database
const db = admin.database();
const ref = db.ref('/prediction');
var startChild = "BBJI";
var batchSize = 10;

function deleteNextBatch() {
  ref.orderByKey()
    .startAt(startChild)
    .limitToFirst(batchSize)
    .once('value')
    .then(function(snapshot) {
      var updates = {};
      snapshot.forEach(function(childSnapshot) {
        updates[childSnapshot.key] = null; // Set to null to delete
        startChild = childSnapshot.key; // Update the startChild marker
      });
      return ref.update(updates);
    })
    .then(function() {
      if (startChild) {
        // Continue deleting the next batch
        deleteNextBatch();
      } else {
        console.log("All data under the node deleted successfully.");
      }
    })
    .catch(function(error) {
      console.error("Error deleting data: " + error);
    });
}

// Start the process
deleteNextBatch();