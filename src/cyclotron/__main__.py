import argparse

import cyclotron.batch as batch
import cyclotron.cycles as cycles
import cyclotron.report as _report


def main():
    p = argparse.ArgumentParser(prog='python -m cyclotron')
    sub = p.add_subparsers(dest='cmd', required=True)

    run_p = sub.add_parser('run', help='single benchmark run')
    cycles.add_args(run_p)

    batch_p = sub.add_parser(
        'batch', help='sweep across parameter grid for multiple executables'
    )
    batch.add_args(batch_p)

    report_p = sub.add_parser(
        'report', help='show absolute numbers for one result file'
    )
    report_p.add_argument('input', help='JSON result file')
    report_p.add_argument(
        '--sort',
        default='cycle_size,extra_bytes,live_objects',
        help='comma-separated keys to sort by',
    )

    compare_p = sub.add_parser(
        'compare', help='compare base vs new result files side-by-side'
    )
    compare_p.add_argument('base', help='baseline JSON result file')
    compare_p.add_argument('new', help='new JSON result file')
    compare_p.add_argument(
        '--sort',
        default='cycle_size,extra_bytes,live_objects',
        help='comma-separated keys to sort by',
    )

    args = p.parse_args()
    if args.cmd == 'run':
        cycles.main(args)
    elif args.cmd == 'batch':
        batch.main(args)
    elif args.cmd == 'report':
        _report.report(args)
    elif args.cmd == 'compare':
        _report.compare(args)


if __name__ == '__main__':
    main()
