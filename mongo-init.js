// Runs once, on first start with an empty data volume (mongo image's /docker-entrypoint-initdb.d).
// Creates a least-privilege user for the app so the API never connects as root.
const dbName = process.env.MONGO_DB || "vod_platform";
const user = process.env.MONGO_APP_USER;
const pwd = process.env.MONGO_APP_PASSWORD;

if (!user || !pwd) {
  throw new Error("MONGO_APP_USER and MONGO_APP_PASSWORD must be set");
}

db.getSiblingDB(dbName).createUser({
  user: user,
  pwd: pwd,
  roles: [{ role: "readWrite", db: dbName }], // includes createIndex, needed at API start-up
});
