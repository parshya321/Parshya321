
const mongoose = require('mongoose');

const episodeSchema = new mongoose.Schema({
  ep_num: { type: Number, required: true },
  title: String,
  stream_url: { type: String, required: true }
});

const animeSchema = new mongoose.Schema({
  anime_id: { type: String, required: true, unique: true },
  title: { type: String, required: true },
  poster: String,
  banner: String,
  synopsis: String,
  genres: [String],
  status: String,
  episodes: [episodeSchema],
  updated_at: { type: Date, default: Date.now }
});

module.exports = mongoose.model('Anime', animeSchema);
