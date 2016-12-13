import collections
import datetime
import os
import zipfile

from flask import Flask, render_template, request, redirect, send_from_directory
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///benchmarks.db'
db = SQLAlchemy(app)

SAVE_PREFIX = './files'
GRAPH_PREFIX = './graphs'

import processing

class BenchGroup(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime)
    disk_model = db.Column(db.Text)
    benchmarks = db.relationship('Benchmark', backref='benchgroup', lazy='select')
    fdisk = db.Column(db.Text)

class Benchmark(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filesystem = db.Column(db.Text)
    bcache_commit = db.Column(db.Text)
    benchgroup_id = db.Column(db.Integer, db.ForeignKey('bench_group.id'))

db.create_all()

@app.route('/graphs/<path:path>')
def graph(path):
    return send_from_directory('graphs', path)

@app.route('/')
def index():
    benchgroups = BenchGroup.query.order_by(BenchGroup.date.desc()).limit(100).all()
    benchgroups = [
        {
            'id': bg.id,
            'object': bg,
            'date': bg.date,
            'disk_model': bg.disk_model,
            'filesystems': sorted([(b.filesystem, b.id) for b in bg.benchmarks], key=lambda x: x[0])
        } for bg in benchgroups
    ]
    for bg in benchgroups:
        for b in bg['object'].benchmarks:
            if b.filesystem == 'bcache':
                bg['bcache_commit'] = b.bcache_commit
    return render_template('index.html', benchgroups=benchgroups)

@app.route('/submit', methods=['POST'])
def submit():
    print(request.files)
    if 'benchfile' not in request.files:
        return 'Bad request, no file', 400

    benchfile = request.files['benchfile']

    if benchfile.filename.split('.')[-1] != 'zip':
        return 'Wrong file extension. Not .zip', 400

    with zipfile.ZipFile(benchfile) as z:
        filenames = set(z.namelist())
        required_names = {'model_number.txt', 'date.txt', 'bcache_commit.txt', 'fdisk.txt'}

        if required_names.difference(filenames):
            return 'Missing files in ZIP: {}'.format(', '.join(required_names.difference(filenames))), 400

        with z.open('model_number.txt', 'r') as infile:
            disk_model = infile.read().decode('UTF-8')

        with z.open('date.txt', 'r') as infile:
            date_str = infile.read().decode('UTF-8')

        with z.open('bcache_commit.txt', 'r') as infile:
            bcache_commit = infile.read().decode('UTF-8')

        with z.open('fdisk.txt', 'r') as infile:
            fdisk = infile.read().decode('UTF-8')

        bg = BenchGroup()
        bg.disk_model = disk_model.strip()
        bg.date = datetime.datetime.fromtimestamp(float(date_str.strip()))
        bg.fdisk = fdisk
        db.session.add(bg)
        db.session.commit()
        
        filesystems = set()
        filesystem_files = collections.defaultdict(lambda: [])
        for filename in filenames:
            if '-' not in filename:
                continue
            filesystem = filename.split('-')[0]
            filesystems.add(filesystem)
            filesystem_files[filesystem].append(filename)

        filesystems = {filename.split('-')[0] for filename in filenames if '-' in filename}
        benchmark_objects = {}
        for filesystem in filesystems:
            b = Benchmark()
            b.benchgroup = bg
            b.filesystem = filesystem
            if filesystem == 'bcache':
                b.bcache_commit = bcache_commit
            db.session.add(b)
            benchmark_objects[filesystem] = b
        db.session.commit()

        for filesystem, filenames in filesystem_files.items():
            b = benchmark_objects[filesystem]
            for filename in filenames:
                output_filename = str(b.id) + '-' + filename
                with z.open(filename, 'r') as infile, open(os.path.join(SAVE_PREFIX, output_filename), 'wb') as outfile:
                    outfile.write(infile.read())

        for b in benchmark_objects.values():
            processing.process_benchmark(b.id, b.filesystem, SAVE_PREFIX, GRAPH_PREFIX)

        return redirect('/view/{}'.format(bg.id))

@app.route('/view/<int:bg_id>')
def view(bg_id):
    bg = BenchGroup.query.filter_by(id=bg_id).first_or_404()
    return render_template('view_group.html', bg=bg, tests=processing.tests, metrics=processing.metrics, GRAPH_PREFIX=GRAPH_PREFIX)

@app.route('/view/bench/<int:b_id>')
def view_single(b_id):
    b = Benchmark.query.filter_by(id=b_id).first_or_404()
    return render_template('view_single.html', b=b, tests=processing.tests, metrics=processing.metrics, GRAPH_PREFIX=GRAPH_PREFIX)
