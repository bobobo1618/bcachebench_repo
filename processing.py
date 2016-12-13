import csv, pandas as pd, itertools, os
import matplotlib, matplotlib.pyplot as plt, matplotlib.colors

matplotlib.use('Agg')

tests = ['seqread', 'seqwrite', 'randread', 'randwrite', '70mix-read', '70mix-write']
metrics = [
    ('bw.bw', 'bw', 'KB/s', 'bandwidth'), 
    ('iops.iops', 'iops', 'op/s', 'IOPS'),
    ('lat.lat', 'lat', 'usec', 'latency'),
]
filename_format = '{id}-{fs}-{test}_{metric1}.log_{metric2}.log'

def get_set(log_prefix, bench_id, fs, test, metric):
    metric1, metric2 = tuple(metric.split('.'))
    direction = None
    if '-' in test:
        test, direction = tuple(test.split('-'))
        direction = 0 if direction == 'read' else 1
    ds_filename = os.path.join(log_prefix, filename_format.format(id=bench_id, fs=fs, test=test, metric1=metric1, metric2=metric2))
    ds = pd.read_csv(ds_filename, index_col=0, names=['time', metric2, 'direction', 'blocksize'])
    if direction is not None:
        ds = ds[ds.direction == direction].copy()
    return ds

def plot(ds, fs, metric='bw', unit='KB/s', title=None):
    if metric.endswith('lat'):
        plt.figure(fs, figsize=(10,6), dpi=80)
        if title:
            plt.title(fs + ' ' + title)
        std = ds.lat.std()
        avg = ds.lat.mean()
        valmax = avg + 1.5*std
        ps = ds.lat[ds.lat < valmax]
        plt.hist2d(ps.index/1000, ps, bins=80)
        plt.plot((0, ps.index.max()//1), (avg, avg), color='orange', linewidth=5.0)
        plt.plot((0, ps.index.max()//1), (avg+std, avg+std), color='orange', linewidth=2.0)
        plt.colorbar()
        plt.figtext(0, 0, "{} mean: {:.3f} {unit}, stddev: {:.3f} {unit}".format(fs, ds[metric].mean(), ds[metric].std(), unit=unit))
        plt.show()
    else:
        plt.figure(figsize=(10,6), dpi=80)
        
        if title:
            plt.title(title)
        
        pointsize = 3.0 if len(ds[metric]) < 1000 else 0.2

        avg = ds[metric].mean()
        stddevx2 = ds[metric].std()
        plt.scatter(ds.index, ds[metric], c='blue', edgecolors='blue', s=pointsize)
        plt.plot((ds.index.min(), ds.index.max()), (avg, avg), color='darkblue', linewidth=3.0)
        plt.plot((ds.index.min(), ds.index.max()), (avg + stddevx2, avg + stddevx2), color='darkblue', linewidth=3.0)
        plt.plot((ds.index.min(), ds.index.max()), (avg - stddevx2, avg - stddevx2), color='darkblue', linewidth=3.0)

        plt.xlim(ds.index.min(), ds.index.max())
        plt.ylim(0, ds[metric].quantile(0.99))
    
        plt.figtext(0, 0, "{} mean: {:.3f} {unit}, stddev: {:.3f} {unit}".format(fs, ds[metric].mean(), ds[metric].std(), unit=unit))
        plt.show()

def process_benchmark(bench_id, filesystem, log_prefix, graph_prefix):
    for test in tests:
        for metric, short, unit, title in metrics:
            ds = get_set(log_prefix, bench_id, filesystem, test, metric)
            plot(ds, filesystem, metric=short, unit=unit, title=test.title() + ' ' + title)
            filename = '{}-{}-{}-{}.png'.format(bench_id, filesystem, test, metric)
            filepath = os.path.join(graph_prefix, filename)
            plt.savefig(filepath)
            plt.close()