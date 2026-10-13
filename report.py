"""Generate the TP report and figures exclusively from verified measurements."""
from pathlib import Path
import html
import json
import statistics
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak

ROOT = Path(__file__).resolve().parent
LABELS = {'eager': 'Pandas - full load', 'stream': 'Pandas - 100k chunks', 'lazy': 'Dask - parallel'}
PALETTE = {'eager': '#A53E40', 'stream': '#237A69', 'lazy': '#5361AC'}
SOURCES = [
    ('Dataset and download', 'https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/data'),
    ('Dataset terms', 'https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/rules'),
    ('Pandas CSV / chunksize', 'https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html'),
    ('Dask partition design', 'https://docs.dask.org/en/stable/dataframe-design.html'),
    ('Dask optimizer', 'https://docs.dask.org/en/stable/dataframe-optimizer.html'),
    ('Windows Job Objects', 'https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects')]


def make_figures(runs, destination):
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                         'axes.spines.top': False, 'axes.spines.right': False,
                         'axes.labelcolor': '#334155', 'text.color': '#172B3A'})
    complete = [m for m in LABELS if any(r['method'] == m and r['status'] == 'ok' for r in runs)]
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8), layout='constrained')
    durations = [statistics.median(r['processing_seconds'] for r in runs if r['method'] == m and r['status'] == 'ok') for m in complete]
    bars = axes[0].bar(range(len(complete)), durations, color=[PALETTE[m] for m in complete], width=.5)
    axes[0].bar_label(bars, fmt='%.1f s', padding=5)
    axes[0].set_xticks(range(len(complete)), [LABELS[m].replace(' - ', '\n') for m in complete])
    axes[0].set_ylim(0, max(durations)*1.25)
    axes[0].set_ylabel('Processing seconds')
    axes[0].set_title('Completed processing only', loc='left', fontweight='bold')
    methods = list(LABELS)
    peaks = [statistics.median(r['peak_rss_bytes'] / 1024**2 for r in runs if r['method'] == m) for m in methods]
    bars = axes[1].bar(range(3), peaks, color=[PALETTE[m] for m in methods], width=.5)
    axes[1].bar_label(bars, fmt='%.0f MiB', padding=5)
    axes[1].set_xticks(range(3), [LABELS[m].replace(' - ', '\n') for m in methods])
    axes[1].set_ylim(0, max(peaks)*1.25)
    axes[1].set_ylabel('Peak sampled RSS (MiB)')
    axes[1].set_title('Median of per-run peaks', loc='left', fontweight='bold')
    fig.savefig(destination / 'performance.png', dpi=180, facecolor='white')
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(10, 4), layout='constrained')
    for method in LABELS:
        trace = pd.read_csv(ROOT / 'results' / f'{method}_1_memory.csv')
        ax.plot(trace.iloc[:, 0], trace['rss_bytes']/1024**2, color=PALETTE[method], label=LABELS[method], linewidth=1.6)
    ax.set_xlabel('Seconds since fresh process launch (includes imports)')
    ax.set_ylabel('Process-tree resident memory (MiB)')
    ax.set_title('Memory evolution - first recorded repeat', loc='left', fontweight='bold')
    ax.legend(frameon=False, loc='upper right')
    ax.grid(alpha=.15)
    fig.savefig(destination / 'memory_trace.png', dpi=180, facecolor='white')
    plt.close(fig)
    graph = next(r['graph'] for r in runs if r['method'] == 'lazy' and r['status'] == 'ok')
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.5), layout='constrained', gridspec_kw={'width_ratios': [1, 1.4]})
    bars = axes[0].bar(['Before', 'After'], [graph['tasks_before'], graph['tasks_after']], color=['#95A5B3', PALETTE['lazy']], width=.5)
    axes[0].bar_label(bars, padding=4)
    axes[0].set_ylim(0, max(graph['tasks_before'], graph['tasks_after'])*1.25)
    axes[0].set_ylabel('Distinct tasks in union of four graphs')
    axes[0].set_title('Explicit expression optimization', loc='left', fontweight='bold')
    axes[1].set_xlim(0, 1)
    axes[1].set_ylim(0, 1)
    axes[1].axis('off')
    stages = [f"CSV: {graph['partitions']} partitions", 'Parse selected inputs per query',
              'Local counts / sums / fare groups', 'Combine reductions', 'Small final summary']
    for i, label in enumerate(stages):
        y = .88 - i*.19
        axes[1].text(.5, y, label, ha='center', va='center', fontsize=10,
                     bbox={'boxstyle': 'round,pad=.5', 'facecolor': '#EEF1F8', 'edgecolor': '#B5BDD7'})
        if i < len(stages)-1:
            axes[1].annotate('', xy=(.5, y-.125), xytext=(.5, y-.055), arrowprops={'arrowstyle':'->', 'color':'#5361AC'})
    fig.savefig(destination / 'lazy_execution.png', dpi=180, facecolor='white')
    plt.close(fig)


def main():
    results = json.loads((ROOT / 'results' / 'summary.json').read_text())
    environment = json.loads((ROOT / 'results' / 'environment.json').read_text())
    manifest = json.loads((ROOT / 'data' / 'manifest.json').read_text())
    runs = results['runs']
    assert results['validation']['passed'] and results['validation']['full_input']
    assert all(r['status'] in ('ok', 'memory_error') for r in runs)
    figures = ROOT / 'figures'
    figures.mkdir(exist_ok=True)
    make_figures(runs, figures)
    report_dir = ROOT / 'report'
    report_dir.mkdir(exist_ok=True)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle('Lead', parent=styles['Normal'], fontSize=12, leading=18, textColor=colors.HexColor('#334155'), spaceAfter=12))
    styles['Normal'].fontSize = 10
    styles['Normal'].leading = 15
    styles['Normal'].spaceAfter = 9
    styles['Heading1'].textColor = colors.HexColor('#172B3A')
    styles['Heading1'].fontSize = 22
    styles['Heading1'].leading = 28
    styles['Heading2'].fontSize = 13
    styles['Heading2'].leading = 18
    styles['Heading2'].textColor = colors.HexColor('#237A69')
    styles['Title'].fontSize = 30
    styles['Title'].leading = 36
    styles['Title'].alignment = TA_LEFT
    styles.add(ParagraphStyle('SmallText', parent=styles['Normal'], fontSize=8, leading=11, spaceAfter=4))
    story = []
    markdown = []

    def title(text, first=False):
        story.append(Paragraph(html.escape(text), styles['Title' if first else 'Heading1']))
        markdown.append('# ' + text + '\n')

    def paragraph(text, style='Normal'):
        story.append(Paragraph(html.escape(text), styles[style]))
        markdown.append(text + '\n')

    def heading(text):
        story.append(Paragraph(html.escape(text), styles['Heading2']))
        markdown.append('## ' + text + '\n')

    def table(rows, widths=None):
        data = [[Paragraph(html.escape(str(v)), styles['SmallText']) for v in row] for row in rows]
        item = Table(data, colWidths=widths, repeatRows=1, hAlign='LEFT')
        item.setStyle(TableStyle([('BACKGROUND', (0,0), (-1,0), colors.HexColor('#E8EFF0')),
                                 ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#F6F8FA')]),
                                 ('VALIGN', (0,0), (-1,-1), 'TOP'),
                                 ('BOTTOMPADDING', (0,0), (-1,-1), 6), ('TOPPADDING', (0,0), (-1,-1), 6),
                                 ('LINEBELOW', (0,0), (-1,0), .6, colors.HexColor('#B6C9CE'))]))
        story.extend([item, Spacer(1, 10)])
        markdown.append('| ' + ' | '.join(str(v) for v in rows[0]) + ' |')
        markdown.append('| ' + ' | '.join('---' for _ in rows[0]) + ' |')
        markdown.extend('| ' + ' | '.join(str(v) for v in row) + ' |' for row in rows[1:])
        markdown.append('')

    def figure(name, height):
        story.append(Image(str(figures / name), width=170*mm, height=height*mm))
        story.append(Spacer(1, 8))
        markdown.append(f'![{name}](../figures/{name})\n')

    def new_page():
        story.append(PageBreak())

    summary = next(r['summary'] for r in runs if r['method'] == 'stream' and r['status'] == 'ok')
    graph = next(r['graph'] for r in runs if r['method'] == 'lazy' and r['status'] == 'ok')
    chunk_record = next(r for r in runs if r['method'] == 'stream' and r['status'] == 'ok')
    title('TP02\nTaxi fares beyond RAM', first=True)
    paragraph('Big Data Analytics | University of Eloued\nSecond-year Master: IA & Data Science | 2026/2027', 'Lead')
    paragraph('A complete CSV experiment comparing full Pandas loading, incremental Pandas processing and lazy parallel Dask computation. The report uses measurements from this project, including the actual memory failure.')
    heading('Verified input')
    table([['Property', 'Verified value'],
           ['Dataset', manifest['dataset']], ['Training rows', f"{manifest['physical_rows']:,}"],
           ['CSV on disk', f"{manifest['csv_bytes']:,} bytes = {manifest['csv_bytes']/1e9:.3f} GB = {manifest['csv_bytes']/1024**3:.3f} GiB"],
           ['Downloaded ZIP', f"{manifest['archive']['bytes']:,} bytes"],
           ['Integrity', 'ZIP CRC verified; CSV SHA-256 recorded in data/manifest.json'],
           ['Data handling', 'Original CSV, all rows, all eight columns. No filtering, duplication or synthetic inflation.']], [42*mm,128*mm])
    heading('Schema and purpose')
    paragraph('The eight fields are key, fare_amount, pickup_datetime, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude and passenger_count. The two text columns use Python strings; the six numeric fields use float64 consistently across the three methods. Float64 also accommodates missing numeric values.')
    paragraph('The experiment calculates valid counts for all columns, numeric sums and global means, plus fares grouped by passenger count. It does not train a prediction model, because model training is outside TP02.')
    paragraph('Negative fares, unusual passenger counts and invalid-looking coordinates are retained. These are raw-data summaries; data quality affects interpretation, not the comparison between methods.', 'SmallText')
    new_page()
    title('01 / Methods and controls')
    heading('Task 1 - full Pandas loading')
    paragraph('analysis.eager calls pd.read_csv on the entire CSV before computing the aggregates. It uses the Python parser because two preliminary full-load trials with the C parser exited with native Windows access-violation status under the cap, without a Python traceback. The exact native cause is not established. Each strategy runs in a fresh process. A Windows Job Object imposes the same 2,048 MiB committed-memory budget on every measured worker, so the full-load attempt demonstrates a real allocation failure without exhausting the whole computer.')
    failures = [r for r in runs if r['method']=='eager' and r['status']=='memory_error']
    if failures:
        paragraph(f"Observed outcome: MemoryError in {len(failures)} of {environment['repeats']} full-load attempts. The exact exception and traceback are retained in results/eager_*.json and results/eager_*.log. The duration is time until failure, not time to process all rows.")
        explanation = failures[0]['message'][:240] or 'allocation failed during pd.read_csv; see the saved traceback.'
        paragraph(f"First exception: {failures[0]['exception_type']}: {explanation}", 'SmallText')
    else:
        paragraph('The full-load method completed in this configuration. Its successful result is compared with the other methods; no OOM result has been invented.')
    heading('Task 2 - 100,000-row Pandas chunks')
    paragraph(f"analysis.stream consumes {chunk_record['chunks']} chunks, never retaining the input chunks. Each has at most 100,000 rows; the final chunk has {summary['rows'] % 100_000:,} rows. RunningTotals keeps column counts, a compensated sum vector and a small table of passenger groups.")
    paragraph('Global mean = accumulated sum / accumulated valid count. Averaging the means of equally weighted chunks would be incorrect when the last chunk is smaller or chunks have different missing-value counts. Kahan accumulation reduces floating-point error while retaining only small numeric vectors.')
    heading('Task 3 - lazy parallel Dask')
    paragraph(f"Dask reads the CSV in {environment['block_mib']} MiB blocks, builds reductions without materializing the complete table, and executes them with {environment['threads']} threads. The graph is explicitly optimized before compute. All eight columns contribute to valid counts, so the aggregate workload includes the complete schema.")
    heading('Fair measurement and practical limits')
    paragraph(f"Machine: {environment['total_ram_bytes']/1024**3:.2f} GiB total RAM, {environment['cpu_physical']} physical / {environment['cpu_logical']} logical CPU cores. The 2 GiB budget is a controlled process allocation limit, not this machine's physical memory capacity. Full Pandas may succeed if that limit is removed.")
    paragraph('Processing time excludes startup and imports, includes parsing and aggregation, and includes graph construction for Dask. psutil samples process-tree RSS and private memory every 100 ms, including imports. A launcher may cause double counting of shared pages. Sampled peaks can miss short spikes. Completed speed comparison is between chunking and Dask, which share the C parser; full-load time is time to failure using the Python parser.')
    new_page()
    title('02 / Performance benchmark')
    medians = {}
    comparison = [['Method', 'Outcome', 'Median time', 'Median peak RSS']]
    for method in LABELS:
        subset = [r for r in runs if r['method']==method]
        medians[method] = statistics.median(r['processing_seconds'] for r in subset)
        complete = all(r['status']=='ok' for r in subset)
        comparison.append([LABELS[method], 'Completed' if complete else 'MemoryError',
                           f"{medians[method]:.2f} s" + ('' if complete else ' to failure'),
                           f"{statistics.median(r['peak_rss_bytes']/1024**2 for r in subset):.1f} MiB"])
    table(comparison,[57*mm,32*mm,41*mm,40*mm])
    figure('performance.png', 67)
    paragraph('Three repetitions per method. Each repeat rotates method order to reduce systematic ordering effects. The operating-system file cache is not flushed; these measurements describe this machine and run configuration, rather than a universal speed ranking.', 'SmallText')
    detail = [['Run', 'Status', 'Seconds', 'Peak RSS MiB']]
    for row in sorted(runs, key=lambda r:(list(LABELS).index(r['method']),r['repeat'])):
        detail.append([f"{row['method']} #{row['repeat']}",row['status'],f"{row['processing_seconds']:.2f}",f"{row['peak_rss_bytes']/1024**2:.1f}"])
    table(detail,[40*mm,48*mm,40*mm,42*mm])
    ratio = medians['stream'] / medians['lazy']
    paragraph(f"In these measurements, Dask was {ratio:.2f}x as fast as chunking (chunking time / Dask time). A value below 1 means chunking was faster. Dask has scheduling, graph and parsing overhead; its expected advantage must be checked experimentally.")
    new_page()
    title('03 / Memory and lazy execution')
    figure('memory_trace.png', 68)
    paragraph('The trace ends when each first-repeat process exits. A full-load failure trace is incomplete processing; the successful chunking and Dask traces represent all 55,423,856 records. Their working memory depends on chunk/partition size, thread concurrency, parser temporaries and the accumulated group table.')
    heading('Lazy evaluation and optimization')
    paragraph('read_csv reads a small sample to infer metadata and creates partition tasks. It does not immediately load every partition. Counts, sums and groupby create a graph of dependencies. Execution begins when dask.compute is called; the final output is small and can fit in RAM.')
    figure('lazy_execution.png', 60)
    paragraph(f"Recorded graph: {graph['partitions']} partitions; {graph['tasks_before']} distinct tasks before optimization and {graph['tasks_after']} after. The figure on the right is a conceptual execution diagram, not an exact plot of every task. Native expression optimization can fuse tasks and project query inputs. Counts across all eight columns still require the full schema; separate reductions may parse shared CSV inputs more than once.")
    paragraph('All four queries are submitted together to the threaded scheduler. Tasks with identical keys can be shared. Optimizing each expression separately can limit sharing and is one possible source of overhead. The task count describes the union of the four explicitly constructed graphs, before scheduler execution.', 'SmallText')
    new_page()
    title('04 / Results and validation')
    table([['Metric', 'Value'], ['Processed rows', f"{summary['rows']:,}"],
           ['Fare sum (USD)', f"{summary['numeric']['fare_amount']['sum']:,.2f}"],
           ['Mean fare (USD)', f"{summary['numeric']['fare_amount']['mean']:.8f}"],
           ['Passenger count sum', f"{summary['numeric']['passenger_count']['sum']:,.0f}"],
           ['Mean passengers', f"{summary['numeric']['passenger_count']['mean']:.8f}"]], [85*mm,85*mm])
    heading('Fare groups')
    groups = [['Passengers', 'Valid fares', 'Mean USD', 'Passengers', 'Valid fares', 'Mean USD']]
    items = summary['fares_by_passengers']
    half = (len(items) + 1) // 2
    def group_cells(group):
        return ['Missing' if group['passengers'] is None else f"{group['passengers']:g}",
                f"{group['valid_fares']:,}", '-' if group['mean_fare'] is None else f"{group['mean_fare']:.6f}"]
    for index in range(half):
        groups.append(group_cells(items[index]) + (group_cells(items[index+half]) if index+half<len(items) else ['', '', '']))
    table(groups,[22*mm,31*mm,32*mm,22*mm,31*mm,32*mm])
    paragraph(f"Validation compared {results['validation']['successful_runs_compared']} successful full-dataset runs: integer counts match exactly; numeric sums and means match with relative tolerance 1e-10 and absolute tolerance 1e-6. The complete parsed row count also matches the extraction manifest. All details are in results/validation.json and results/summary.json.")
    paragraph('test_analysis.py checks a hand-calculated fixture with negative fares, missing values, passenger groups and unequal final chunks. It confirms the global sum and mean, rather than testing only agreement between implementations.')
    heading('Conclusion')
    paragraph('Under the declared memory budget, incremental and partitioned computation can process a CSV larger than available worker memory. Pandas chunking retains one batch plus summaries; Dask schedules partition work and combines reductions. Execution speed and resident memory remain empirical measurements, not guaranteed properties.')
    new_page()
    title('05 / Reproduction and submission')
    heading('Run from the project directory')
    for command in ['python -m venv .venv', '.\\.venv\\Scripts\\python.exe -m pip install -r requirements.txt',
                    '.\\.venv\\Scripts\\python.exe prepare.py', '.\\.venv\\Scripts\\python.exe -m unittest test_analysis -v',
                    f".\\.venv\\Scripts\\python.exe benchmark.py --repeats 3 --budget-mib 2048 --threads 4 --block-mib {environment['block_mib']}",
                    '.\\.venv\\Scripts\\python.exe report.py']:
        paragraph(command, 'SmallText')
    paragraph('Download the full Kaggle competition ZIP into data/ first. Sign in and accept the competition rules on your own account. prepare.py extracts only train.csv and verifies size, header, CRC during extraction, SHA-256 and row count. Existing files are reverified. The exact installed environment is also recorded in requirements.lock.txt.')
    heading('Evidence and source files')
    table([['File / folder', 'Purpose'], ['analysis.py', 'Full load, streaming accumulator, native lazy reductions'],
           ['benchmark.py', 'Fresh workers, Windows budget, time and psutil profiling'],
           ['prepare.py + data/manifest.json', 'Reproducible extraction and integrity evidence'],
           ['results/', 'Every run, exception, RSS trace, environment and validation'],
           ['figures/ + report/', 'Measured charts and this report'],
           ['README.md + run.ps1', 'Usage and submission instructions']], [68*mm,102*mm])
    paragraph('The TP asks for work to be uploaded to GitHub. Code, reports, figures and benchmark evidence can be committed. The original CSV, ZIP and environment are excluded from Git. Competition rules allow educational use but restrict redistribution of raw competition data; each reader obtains it through their own Kaggle account.')
    paragraph('Preliminary full-load trials using the C parser exited natively with status 0xC0000005 under the allocation cap, first with Arrow strings and then with object strings. No Python traceback established the exact native cause. Diagnostics are in results/pilot_arrow/ and results/pilot_cparser/, excluded from the final comparison. Final measurements use object strings; full-load uses the Python parser and both successful methods use the C parser.', 'SmallText')
    heading('References')
    for label, url in SOURCES:
        story.append(Paragraph(f'<link href="{url}" color="#5361AC">{html.escape(label)}</link>', styles['SmallText']))
        markdown.append(f'- [{label}]({url})')
    def page_number(canvas, document):
        canvas.setStrokeColor(colors.HexColor('#CFDBDF'))
        canvas.line(20*mm, 16*mm, 190*mm, 16*mm)
        canvas.setFont('Helvetica',8)
        canvas.setFillColor(colors.HexColor('#657782'))
        canvas.drawString(20*mm, 11*mm, 'TP02 | Taxi fare CSV | Big Data Analytics')
        canvas.drawRightString(190*mm, 11*mm, str(document.page))
    document = SimpleDocTemplate(str(report_dir / 'TP02_Taxi_Fares.pdf'), pagesize=A4,
                                 rightMargin=20*mm,leftMargin=20*mm,topMargin=20*mm,bottomMargin=23*mm,
                                 title='TP02 - Taxi fares beyond RAM',author='Big Data Analytics TP02')
    document.build(story,onFirstPage=page_number,onLaterPages=page_number)
    (report_dir / 'REPORT.md').write_text('\n'.join(markdown) + '\n', encoding='utf-8')
    print('Report and three figures generated from verified results.')


if __name__ == '__main__':
    main()
