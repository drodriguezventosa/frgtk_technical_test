import time
import subprocess
import sys


def run_version(script_name):
    """Run a script and capture its output, returning elapsed time per phase."""
    print(f"\n{'='*60}")
    print(f"Running: {script_name}")
    print('='*60)

    start = time.perf_counter()
    result = subprocess.run([sys.executable, script_name], capture_output=True, text=True)
    total = time.perf_counter() - start

    output = result.stdout + result.stderr
    print(output)

    if result.returncode != 0:
        print(f"ERROR: {script_name} failed with return code {result.returncode}")
        return None, total

    # Parse phase times from output
    phases = {}
    lines = output.strip().split('\n')
    current_phase = None
    for line in lines:
        if line.startswith(('Init', 'Importing', 'Cleaning', 'Adding', 'Generating week',
                           'Generating month', 'Formatting', 'Exporting')):
            current_phase = line.strip().rstrip('.')
        elif '*** ' in line and ' seconds ***' in line and current_phase:
            t = float(line.split('*** ')[1].split(' seconds')[0])
            phases[current_phase] = t
            current_phase = None

    return phases, total


def compare_csv(file1, file2):
    """Compare two CSV files and report differences."""
    try:
        with open(file1) as f1, open(file2) as f2:
            lines1 = f1.readlines()
            lines2 = f2.readlines()

        if lines1[0] != lines2[0]:
            print(f"  Headers differ: {file1} vs {file2}")
            return False

        if len(lines1) != len(lines2):
            print(f"  Row count differs: {len(lines1)} vs {len(lines2)}")
            return False

        print(f"  {file1} vs {file2}: {len(lines1)} rows, headers match ✓")
        return True
    except FileNotFoundError as e:
        print(f"  File not found: {e}")
        return False


if __name__ == '__main__':
    scripts = {
        'Original (Pandas)': 'main.py',
        'Optimized (Pandas)': 'main_optimized.py',
        'Polars': 'main_polars.py',
    }

    results = {}
    for label, script in scripts.items():
        phases, total = run_version(script)
        results[label] = {'phases': phases or {}, 'total': total}

    # Summary table
    print(f"\n{'='*60}")
    print("BENCHMARK RESULTS")
    print('='*60)

    all_phases = []
    for r in results.values():
        for p in r['phases']:
            if p not in all_phases:
                all_phases.append(p)

    header = f"{'Phase':<30}"
    for label in results:
        header += f" | {label:>20}"
    print(header)
    print('-' * len(header))

    for phase in all_phases:
        row = f"{phase:<30}"
        for label in results:
            t = results[label]['phases'].get(phase, '-')
            row += f" | {t:>20.2f}s" if isinstance(t, float) else f" | {t:>20}"
        print(row)

    row = f"{'TOTAL':<30}"
    for label in results:
        row += f" | {results[label]['total']:>20.2f}s"
    print('-' * len(header))
    print(row)

    # Compare outputs
    print(f"\n{'='*60}")
    print("OUTPUT COMPARISON")
    print('='*60)
    compare_csv('processed_data.csv', 'processed_data_optimized.csv')
    compare_csv('processed_data.csv', 'processed_data_polars.csv')
