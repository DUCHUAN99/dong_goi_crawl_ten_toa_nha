import sys

def print_progress_bar(index, total, label):
    # 
    # :param index:
    # :param total:
    # :param label:
    # :return: Thanh tiến trình
    # 

    n_bar = 50  # Progress bar width
    progress = index / total
    sys.stdout.write('\r')
    sys.stdout.write(f"[{'=' * int(n_bar * progress):{n_bar}s}] {int(100 * progress)}%  {label}")
    sys.stdout.flush()

