const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const admin = require('firebase-admin');

const serviceAccount = require('../credentials.json'); // Replace with your service account credentials
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
app.use(cors());
// Handle incoming socket connections
io.on('connection', (socket) => {
  console.log('A client connected.');

  // Listen for changes in your Firebase database and emit updates to connected clients
  const ref_prediction = db.ref('/prediction_all');
  const ref_prediction_BBJI = db.ref('/prediction/BBJI')
  const ref_prediction_SMRI = db.ref('/prediction/SMRI')
  const ref_prediction_JAGI = db.ref('/prediction/JAGI')

  const ref_waves = db.ref('/waves');
  const ref_waves_BBJI = db.ref('/waves/BBJI')
  const ref_waves_SMRI = db.ref('/waves/SMRI')
  const ref_waves_JAGI = db.ref('/waves/JAGI')

  ref_prediction.limitToLast(1).on('child_added', (snapshot) => {
    const data = snapshot.val();
    // console.log(data.length)
    socket.emit('prediction-data-all', data);
  });

  ref_prediction_BBJI.limitToLast(1).on('child_added', (snapshot) => {
    const data = snapshot.val();
    // console.log(data.length)
    socket.emit('prediction-data-BBJI', data);
  })

  ref_prediction_SMRI.limitToLast(1).on('child_added', (snapshot) => {
    const data = snapshot.val();
    // console.log(data.length)
    socket.emit('prediction-data-SMRI', data);
  })

  ref_prediction_JAGI.limitToLast(1).on('child_added', (snapshot) => {
    const data = snapshot.val();
    console.log("data key : " + snapshot.key)
    console.log(data);
    socket.emit('prediction-data-JAGI', data);
  })

  
  // waves
  ref_waves_BBJI.limitToLast(1).on('child_added', (snapshot) => {
    const data = snapshot.val();
    // console.log(data)
    socket.emit('waves-data-BBJI', data);
  })

  ref_waves_SMRI.limitToLast(1).on('child_added', (snapshot) => {
    const data = snapshot.val();
    // console.log(data.length)
    socket.emit('waves-data-SMRI', data);
  })

  ref_waves_JAGI.limitToLast(1).on('child_added', (snapshot) => {
    const data = snapshot.val();
    // console.log("data key : " + snapshot.key)
    socket.emit('waves-data-JAGI', data);
  })

  // Handle socket disconnect
  socket.on('disconnect', () => {
    console.log('A client disconnected.');
    ref_prediction.off(); // Stop listening to database changes for this client
    ref_prediction_BBJI.off();
    ref_prediction_SMRI.off();
    ref_prediction_JAGI.off();

    ref_waves_BBJI.off();
    ref_waves_SMRI.off();
    ref_waves_JAGI.off();
  });
});

app.get('/', (req, res) => {
  res.sendFile(__dirname + '/public/index.html'); // Adjust the path as needed
});

server.listen(3000, () => {
  console.log('Server is running on port 3000');
});