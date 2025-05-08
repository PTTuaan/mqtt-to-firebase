const mqtt = require('mqtt');
const admin = require('firebase-admin');
const serviceAccount = require('./tte-safe-iot-firebase-adminsdk-fbsvc-0a628687ee.json');

// Khởi tạo Firebase Admin
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});
const db = admin.firestore();

// MQTT broker config
const mqttOptions = {
  username: 'xinruhd',
  password: 'xr621Hd168',
};
const client = mqtt.connect('mqtt://h.ceosz.com:1883', mqttOptions);

// Khi kết nối MQTT thành công, subscribe các topic từ Firestore
client.on('connect', async () => {
  console.log('✅ Connected to MQTT broker');

  try {
    const snapshot = await db.collection('Models').get();
    snapshot.forEach((doc) => {
      const modelId = doc.id;
      const topic = `up/a1JXGupSRBK/${modelId}`;
      client.subscribe(topic, (err) => {
        if (err) {
          console.error(`❌ Không subscribe được topic ${topic}:`, err);
        } else {
          console.log(`📡 Đã subscribe topic: ${topic}`);
        }
      });
    });
  } catch (err) {
    console.error('❌ Lỗi khi lấy model từ Firestore:', err);
  }
});

client.on('message', async (topic, message) => {
  try {
    // Extract modelId từ topic
    const topicParts = topic.split('/');
    const modelId = topicParts[2];

    const data = JSON.parse(message.toString());

    // Chỉ xử lý nếu id là '60006'
    if (!data.id || data.id.toString() !== '60006') return;

    // Lấy deviceId từ payload
    let deviceId = '';
    if (data.params && data.params.DetailMsg) {
      let detailObj = data.params.DetailMsg;
      if (typeof detailObj === 'string') {
        try {
          detailObj = JSON.parse(detailObj);
        } catch (_) {}
      }
      if (detailObj && detailObj.DeviceID) {
        deviceId = detailObj.DeviceID.toString();
      }
    }

    // Nếu không tìm thấy deviceId, bỏ qua và tiếp tục
    if (!deviceId) {
      console.warn('⚠️ Không tìm thấy deviceId trong payload:', data);
      return;
    }

    // 🔍 Lookup userId từ models/{modelId}
    const modelMetaDoc = await db.collection('Models').doc(modelId).get();
    if (!modelMetaDoc.exists || !modelMetaDoc.data().userId) {
      console.error('❌ Không tìm thấy userId cho modelId:', modelId);
      return;  // Bỏ qua nếu không tìm thấy userId
    }
    const foundUserId = modelMetaDoc.data().userId;

    // Tạo notification object
    const notification = {
      'createdAt': new Date().toISOString(),
      'deviceId': deviceId,
      'id': data.id.toString(),
      'method': data.method ? data.method.toString() : '',
      'modelId': modelId,
      'detailMsg': typeof data.params.DetailMsg === 'string'
        ? data.params.DetailMsg
        : JSON.stringify(data.params.DetailMsg),
      'version': data.version ? data.version.toString() : '',
    };

    // Firestore ref
    const deviceRef = db
      .collection('users')
      .doc(foundUserId)
      .collection('models')
      .doc(modelId)
      .collection('devices')
      .doc(deviceId);

    // Lấy history hiện tại
    const doc = await deviceRef.get();
    let history = [];
    if (doc.exists && doc.data().notificationHistory) {
      history = doc.data().notificationHistory;
    }

    // Thêm vào đầu mảng và giới hạn
    history.unshift(notification);
    const maxHistory = 50;
    history = history.slice(0, maxHistory);

    // Cập nhật Firestore
    await deviceRef.set(
      {
        notificationHistory: history,
        updatedAt: new Date().toISOString(),
      },
      { merge: true }
    );

    console.log(`✅ Notification saved for model ${modelId}, device ${deviceId}`);
  } catch (err) {
    console.error('❌ Error processing MQTT message:', err);
  }
});
