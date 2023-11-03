from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
from spark_job import run_spark_job

app = Flask(__name)

# Specify the directory where uploaded files will be stored
app.config["UPLOAD_FOLDER"] = "uploads"
app.config["ALLOWED_EXTENSIONS"] = set(["csv"])

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in app.config["ALLOWED_EXTENSIONS"]

@app.route("/")
def index():
    return "Welcome to the Spark Web App"

@app.route("/upload", methods=["POST"])
def upload_file():
    if "csvFile" not in request.files:
        return jsonify({"error": "No file part"})

    file = request.files["csvFile"]
    if file.filename == "":
        return jsonify({"error": "No selected file"})

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config["UPLOAD_FOLDER"], filename))
        summary = run_spark_job(os.path.join(app.config["UPLOAD_FOLDER"], filename))
        return jsonify(summary)

if __name__ == "__main__":
    app.run()
