const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
require('dotenv').config();

const Anime = require('./models/Anime');

const app = express();
app.use(express.json());
app.use(cors());

// MongoDB Connection
const MONGO_URI = process.env.MONGO_URI || "mongodb+srv://scrapanimes_db_user:9921019931@cluster0.ahrmz7d.mongodb.net/?retryWrites=true&w=majority";

mongoose.connect(MONGO_URI)
  .then(() => console.log("📦 Server connected to MongoDB Atlas"))
  .catch(err => console.error("❌ MongoDB connection error:", err));

// --- API Endpoints ---

// 1. Saare anime ki list lene ke liye (Catalog)
app.get('/api/anime', async (req, res) => {
  try {
    const animes = await Anime.find({}, 'anime_id title poster genres status updated_at');
    res.json({ success: true, count: animes.length, data: animes });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 2. Kisi specific anime ki poori details aur episodes ke liye
app.get('/api/anime/:anime_id', async (req, res) => {
  try {
    const anime = await Anime.findOne({ anime_id: req.params.anime_id });
    if (!anime) {
      return res.status(404).json({ success: false, message: "Anime not found" });
    }
    res.json({ success: true, data: anime });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Server Start
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server is running on port ${PORT}`);
});
