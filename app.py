from flask import Flask, render_template, request, send_file, jsonify
from werkzeug.utils import secure_filename
import os
import shutil
from data_lake import DataLake

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Initialize Data Lake
data_lake = DataLake("data_lake")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    folder = request.form.get('folder', 'transactions')
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
        
    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    # Save uploaded file temporarily
    file.save(filepath)
    
    # Process file through data lake
    try:
        data_lake.copy_to_raw(filepath, folder)
        data_lake.process_raw_data(folder)
        os.remove(filepath)  # Clean up temp file
        return jsonify({'message': 'File processed successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/view/<path:file_path>')
def view_data(file_path):
    try:
        if (file_path.startswith('processed/')):
            # Remove 'processed/' prefix if present
            folder = os.path.dirname(file_path[10:])
            file_name = os.path.splitext(os.path.basename(file_path))[0]
            df = data_lake.get_processed_data(folder, file_name)
            return render_template('view.html', data=df.to_html(classes='table table-striped'))
        else:
            # For raw files, show file details
            full_path = os.path.join('data_lake', file_path)
            if os.path.exists(full_path):
                with open(full_path, 'r') as f:
                    content = f.read()
                return render_template('view.html', 
                                    data=f'<pre class="bg-light p-3">{content}</pre>')
    except Exception as e:
        return render_template('view.html', 
                             data=f'<div class="alert alert-danger">Error: {str(e)}</div>')

@app.route('/files')
def list_files():
    files = []
    for folder in ['raw', 'processed']:
        path = os.path.join('data_lake', folder)
        for root, _, filenames in os.walk(path):
            for filename in filenames:
                files.append({
                    'name': filename,
                    'path': os.path.join(folder, filename),  # Keep folder/filename format for backend
                    'zone': folder
                })
    return jsonify(files)

@app.route('/delete/<path:file_path>', methods=['DELETE'])
def delete_file(file_path):
    try:
        # Construct full path similar to view function
        full_path = os.path.join('data_lake', file_path)
        print(f"Attempting to delete: {full_path}")  # Debug print
        
        if os.path.exists(full_path):
            os.remove(full_path)
            return jsonify({'message': f'File {file_path} deleted successfully'})
        return jsonify({'error': f'File not found: {full_path}'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/move', methods=['POST'])
def move_file():
    try:
        source = request.json.get('source')
        destination = request.json.get('destination')
        if not source or not destination:
            return jsonify({'error': 'Source and destination required'}), 400
        
        # Construct full paths similar to view function
        source_path = os.path.join('data_lake', source)
        dest_path = os.path.join('data_lake', destination)
        
        print(f"Moving from: {source_path} to: {dest_path}")  # Debug print
        
        # Ensure source exists
        if not os.path.exists(source_path):
            return jsonify({'error': f'Source file not found: {source_path}'}), 404
            
        # Create destination directory if needed
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        # Move the file
        shutil.move(source_path, dest_path)
        return jsonify({'message': 'File moved successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/folder', methods=['POST'])
def create_folder():
    try:
        path = request.json.get('path')
        if not path:
            return jsonify({'error': 'Path required'}), 400
            
        full_path = os.path.join('data_lake', path)
        os.makedirs(full_path, exist_ok=True)
        return jsonify({'message': f'Folder {path} created successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/file/<path:file_path>')
def file_details(file_path):
    try:
        full_path = os.path.join('data_lake', file_path)
        if (os.path.exists(full_path)):
            # For JSON files, read and return content
            if file_path.endswith('.json'):
                with open(full_path, 'r') as f:
                    content = f.read()
                return jsonify({'content': content})
            # For other files, return basic info
            return jsonify({
                'name': os.path.basename(file_path),
                'size': os.path.getsize(full_path),
                'modified': os.path.getmtime(full_path)
            })
        return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Add to app.py - new route to get directory structure
@app.route('/directories')
def list_directories():
    directories = []
    for zone in ['raw', 'processed', 'staging']:
        base_path = os.path.join('data_lake', zone)
        for root, dirs, _ in os.walk(base_path):
            rel_path = os.path.relpath(root, 'data_lake')
            directories.append(rel_path)
            for dir in dirs:
                full_path = os.path.join(rel_path, dir)
                directories.append(full_path)
    return jsonify(directories)

if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)