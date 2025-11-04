#!/usr/bin/env python3
"""Керований запуск пайплайна на фіксований час.

Цей скрипт дозволяє запускати пайплайн з app.main.run_pipeline() на задану кількість часу
(в секундах) з можливістю повторення прогонів та пауз між ними. Також підтримується
динамічне встановлення конфігураційних прапорів через аргументи командного рядка.

Приклади:
# 10 хвилин один раз
python -m tools.run_window --duration 600

# 15 хвилин, двічі, з паузою 5 с між прогонами
python -m tools.run_window --duration 900 --repeat 2 --gap 5

# 15 хв з вимкненим шедулером:
python -m tools.run_window --duration 900 --set STAGE2_SCHEDULER_ENABLED=false

# 30 хв і прапори (через --set багаторазово)
python -m tools.run_window --duration 1800 \
  --set STAGE2_SCHEDULER_ENABLED=true \
  --set PROM_GAUGES_ENABLED=true

# 30 хв, двічі, з relaxed-канарейкою:

python -m tools.run_window --duration 1800 --repeat 2 --gap 5 ^
  --set PROM_GAUGES_ENABLED=true ^
  --set SCENARIO_TRACE_ENABLED=true ^
  --set TEST_SCENARIO_SELECTOR_RELAXED=true ^
  --set SCEN_HTF_MIN=0.05 --set SCEN_PULLBACK_PRESENCE_MIN=0.10 ^
  --set SCEN_BREAKOUT_DVR_MIN=0.10 --set SCEN_REQUIRE_BIAS=false ^
  --set SCEN_PULLBACK_ALLOW_NA=true

Примітка щодо Prometheus-метрик:
Ендпоінт /metrics доступний лише допоки процес працює з прапором PROM_GAUGES_ENABLED=true.
Щоб знімати метрики, відкрий інше (паралельне) вікно термінала і опитуй http://localhost:9108/metrics
під час роботи цього вікна. Після завершення duration ендпоінт закривається, і запит повертає помилку.

Якщо треба залишити старі сценарії на 120 с — використовуй tools/run_window з --duration 120 замість дублювати файли.

"""
import argparse
import asyncio
import importlib
import os
import signal
import sys

# шлях до кореня проєкту
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

EXIT_OK = 0
EXIT_TIMEOUT = 124
EXIT_SIGINT = 130


def _parse_sets(pairs: list[str]) -> list[tuple[str, str]]:
    """Парсинг списку пар NAME=VALUE з --set аргументів.

    Args:
        pairs: Список рядків формату "NAME=VALUE"

    Returns:
        Список кортежів (назва, значення)

    Raises:
        SystemExit: Якщо формат пари неправильний
    """
    out = []
    for p in pairs or []:
        if "=" not in p:
            print(f"❌ ПОМИЛКА --set: очікується NAME=VALUE, отримано: {p}", flush=True)
            raise SystemExit(f"--set expects NAME=VALUE, got: {p}")
        k, v = p.split("=", 1)
        parsed_pair = (k.strip(), v.strip())
        out.append(parsed_pair)
        print(f"📋 Парсинг --set: {parsed_pair[0]}={parsed_pair[1]}", flush=True)
    return out


def _apply_sets(pairs: list[tuple[str, str]]) -> None:
    """Застосування налаштувань з --set до os.environ та config.config.

    Спочатку встановлює змінні оточення, потім намагається оновити
    атрибути у config.config з автоматичним приведенням типів:
    - "true"/"false" → bool
    - числа → int/float
    - інше → str

    Args:
        pairs: Список кортежів (назва, значення) для застосування
    """
    if not pairs:
        print("📝 Немає --set налаштувань для застосування", flush=True)
        return

    print(f"⚙️  Застосування {len(pairs)} налаштувань з --set...", flush=True)

    # 1) через os.environ (для читання конфігом/флагами)
    for k, v in pairs:
        # Зверніть увагу: зміна os.environ впливає лише на дочірні процеси та поточний рантайм,
        # але не на вже імпортовані модулі (наприклад, якщо config вже імпортовано — зміни не застосуються автоматично).
        os.environ[k] = v
        print(f"🌍 os.environ[{k}] = '{v}'", flush=True)

    # 2) спроба напряму у config.config (не фейлити, якщо нема)
    try:
        import config.config as cfg  # noqa

        print("📦 Імпорт config.config успішний, оновлюємо атрибути...", flush=True)

        for k, v in pairs:
            # грубий каст у bool/int/float/str
            vv = v
            low = v.lower()

            if low in ("true", "false"):
                vv = low == "true"
                print(f"🔄 {k}: '{v}' → {vv} (bool)", flush=True)
            else:
                try:
                    vv = int(v)
                    print(f"🔄 {k}: '{v}' → {vv} (int)", flush=True)
                except ValueError:
                    try:
                        vv = float(v)
                        print(f"🔄 {k}: '{v}' → {vv} (float)", flush=True)
                    except ValueError:
                        print(f"🔄 {k}: '{v}' → '{vv}' (str)", flush=True)

            try:
                # Перевіряємо, чи існує атрибут
                if hasattr(cfg, k):
                    old_val = getattr(cfg, k)
                    setattr(cfg, k, vv)
                    print(f"✅ config.{k}: {old_val} → {vv}", flush=True)
                else:
                    # Створюємо новий атрибут
                    setattr(cfg, k, vv)
                    print(f"➕ config.{k}: створено = {vv}", flush=True)
            except Exception as e:
                print(f"⚠️  Не вдалося встановити config.{k}: {e}", flush=True)

    except ImportError as e:
        print(f"⚠️  Не вдалося імпортувати config.config: {e}", flush=True)
    except Exception as e:
        print(f"⚠️  Помилка при роботі з config.config: {e}", flush=True)

    print("✨ Застосування --set налаштувань завершено", flush=True)


async def _run_once(duration: int) -> int:
    """Запуск пайплайна на фіксований час з коректним завершенням.

    Створює task для run_pipeline() і очікує завершення одного з:
    - пайплайн завершився сам
    - спрацював таймер (duration секунд)
    - отримано SIGINT/SIGTERM

    Args:
        duration: Максимальна тривалість роботи у секундах

    Returns:
        EXIT_OK (0) - нормальне завершення
        EXIT_TIMEOUT (124) - завершення по таймеру
        EXIT_SIGINT (130) - завершення по сигналу
    """
    print(f"🚀 Запуск пайплайна на {duration} секунд...", flush=True)

    # Ліниве імпортування після застосування --set, щоб ENV вплинуло на app.main (зокрема порт /metrics)
    try:
        app_main = importlib.import_module("app.main")
        run_pipeline = app_main.run_pipeline  # type: ignore[attr-defined]
    except Exception as e:
        print(f"❌ Не вдалося імпортувати app.main: {e}", flush=True)
        return EXIT_OK

    # Створюємо основний task
    task = asyncio.create_task(run_pipeline())
    print("📊 Task пайплайна створено", flush=True)

    # Event для обробки сигналів
    stop = asyncio.Event()

    def _handler(signum: int = 0, frame=None) -> None:
        """Обробник сигналів SIGINT/SIGTERM."""
        print(f"⚠️  Отримано сигнал завершення (sig={signum}), зупиняємо...", flush=True)
        stop.set()

    # Налаштування обробників сигналів (якщо підтримується)
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: _handler(s))
                print(f"📡 Обробник сигналу {sig.name} зареєстровано", flush=True)
            except NotImplementedError:
                print(f"⚠️  Сигнал {sig.name} не підтримується (Windows?)", flush=True)
    except Exception as e:
        print(f"⚠️  Помилка реєстрації обробників сигналів: {e}", flush=True)

    try:
        print(f"⏱️  Очікування завершення (макс. {duration}s)...", flush=True)

        # Створюємо конкуруючі tasks
        timer_task = asyncio.create_task(asyncio.sleep(duration))
        signal_task = asyncio.create_task(stop.wait())

        # Очікуємо першого завершення
        done, pending = await asyncio.wait(
            {task, timer_task, signal_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Відміняємо незавершені tasks
        for p in pending:
            p.cancel()
            try:
                await p
            except asyncio.CancelledError:
                pass

        # Визначаємо причину завершення
        if task in done:
            print("✅ Пайплайн завершився природно", flush=True)
            try:
                result = await task
                print(f"📋 Результат пайплайна: {result}", flush=True)
            except Exception as e:
                print(f"❌ Пайплайн завершився з помилкою: {e}", flush=True)
            return EXIT_OK

        elif timer_task in done:
            print(f"⏰ Таймер спрацював після {duration}s", flush=True)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                print("🛑 Пайплайн скасовано по таймеру", flush=True)
            except Exception as e:
                print(f"⚠️  Помилка при скасуванні пайплайна: {e}", flush=True)
            return EXIT_TIMEOUT

        elif signal_task in done:
            print("📶 Отримано сигнал зупинки", flush=True)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                print("🛑 Пайплайн скасовано по сигналу", flush=True)
            except Exception as e:
                print(f"⚠️  Помилка при скасуванні пайплайна: {e}", flush=True)
            return EXIT_SIGINT

        else:
            print("❓ Невідома причина завершення", flush=True)
            return EXIT_OK

    except KeyboardInterrupt:
        print("⌨️  KeyboardInterrupt (Ctrl+C) отримано", flush=True)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            print("🛑 Пайплайн скасовано через KeyboardInterrupt", flush=True)
        except Exception as e:
            print(f"⚠️  Помилка при скасуванні: {e}", flush=True)
        return EXIT_SIGINT
    except Exception as e:
        print(f"❌ Неочікувана помилка у _run_once: {e}", flush=True)
        task.cancel()
        try:
            await task
        except Exception:
            pass
        return EXIT_OK


async def main() -> None:
    """Головна функція керованого запуску пайплайна.

    Парсить аргументи командного рядка, застосовує налаштування з --set,
    та виконує один або більше прогонів пайплайна з заданими параметрами.

    Підтримувані exit коди:
    - 0 (EXIT_OK): нормальне завершення
    - 124 (EXIT_TIMEOUT): завершення по таймеру
    - 130 (EXIT_SIGINT): завершення по сигналу/Ctrl+C
    """
    print("🔧 Ініціалізація run_window...", flush=True)

    # Гарантуємо UTF-8 для stdout/stderr (Windows PowerShell часто не в UTF-8)
    try:
        os.environ["PYTHONIOENCODING"] = "utf-8"
    except Exception:
        pass
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    ap = argparse.ArgumentParser(
        description="Керований запуск пайплайна на фіксований час",
        epilog=(
            "Приклади:\n"
            "  %(prog)s --duration 600  # 10 хвилин\n"
            "  %(prog)s --duration 900 --repeat 2 --gap 5  # двічі по 15 хв\n"
            "  %(prog)s --duration 1800 --set PROM_GAUGES_ENABLED=true"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    ap.add_argument(
        "--duration",
        type=int,
        required=True,
        help="Тривалість кожного прогону у секундах (обов'язково)",
    )
    ap.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Кількість повторень прогонів (за замовчуванням: 1)",
    )
    ap.add_argument(
        "--gap",
        type=int,
        default=2,
        help="Пауза між повторами у секундах (за замовчуванням: 2)",
    )
    ap.add_argument(
        "--log",
        default=None,
        help=(
            "Опційний шлях до лог-файлу; stdout/stderr буде дзеркалено у файл. "
            "Якщо вказано лише ім'я файлу без каталогу — буде збережено у logs/runs/."
        ),
    )
    ap.add_argument(
        "--set",
        action="append",
        default=[],
        dest="config_sets",  # Явна назва для clarity
        metavar="NAME=VALUE",
        help=(
            "Встановити конфігураційний прапор/налаштування у форматі NAME=VALUE. "
            "Можна вказувати багаторазово. Приклад: --set PROM_GAUGES_ENABLED=true"
        ),
    )

    args = ap.parse_args()

    print("📋 Параметри запуску:", flush=True)
    print(f"   ⏱️  Тривалість: {args.duration}s", flush=True)
    print(f"   🔄 Повторень: {args.repeat}", flush=True)
    print(f"   ⏸️  Пауза між повторами: {args.gap}s", flush=True)
    print(f"   ⚙️  Налаштувань --set: {len(args.config_sets)}", flush=True)

    # Валідація параметрів
    if args.duration <= 0:
        print("❌ ПОМИЛКА: тривалість має бути > 0", flush=True)
        raise SystemExit("Duration must be positive")

    if args.repeat <= 0:
        print("❌ ПОМИЛКА: кількість повторень має бути > 0", flush=True)
        raise SystemExit("Repeat count must be positive")

    if args.gap < 0:
        print("❌ ПОМИЛКА: пауза не може бути від'ємною", flush=True)
        raise SystemExit("Gap cannot be negative")

    print("✅ Валідація параметрів пройшла успішно", flush=True)

    # Застосування конфігураційних налаштувань
    print("🔧 Застосування конфігураційних налаштувань...", flush=True)
    _apply_sets(_parse_sets(args.config_sets))

    # Опційний лог-файл (простий tee для stdout/stderr)
    if args.log:
        try:
            # Якщо передано лише ім'я файлу або відносний шлях без каталогу — скеровуємо у logs/runs/
            lp = args.log
            if not os.path.isabs(lp) and (os.path.dirname(lp) in ("", ".")):
                os.makedirs(os.path.join("logs", "runs"), exist_ok=True)
                lp = os.path.join("logs", "runs", lp)
            log_path = os.path.abspath(lp)
            print(f"🗂️  Логування також у файл: {log_path}", flush=True)

            class _Tee:
                def __init__(self, stream, fileobj):
                    self._stream = stream
                    self._file = fileobj

                def write(self, data):
                    try:
                        self._stream.write(data)
                    finally:
                        self._file.write(data)

                def flush(self):
                    try:
                        self._stream.flush()
                    finally:
                        self._file.flush()

            # Гарантуємо існування каталогу
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            _log_fh = open(log_path, "a", encoding="utf-8", buffering=1)
            sys.stdout = _Tee(sys.stdout, _log_fh)  # type: ignore[assignment]
            sys.stderr = _Tee(sys.stderr, _log_fh)  # type: ignore[assignment]
        except Exception as e:
            print(f"⚠️  Не вдалося увімкнути лог у файл: {e}", flush=True)

    # Виконання прогонів
    print(f"🚀 Початок виконання {args.repeat} прогон(ів)...", flush=True)
    code = EXIT_OK

    for i in range(args.repeat):
        run_num = i + 1
        print(f"\n{'='*60}", flush=True)
        print(
            f"🎯 ПРОГОН {run_num}/{args.repeat}: тривалість {args.duration}s",
            flush=True,
        )
        print(f"{'='*60}", flush=True)

        # Запуск одного прогону
        rc = await _run_once(args.duration)
        code = max(code, rc)  # Зберігаємо найгірший exit код

        print(f"📊 ПРОГОН {run_num} завершено з кодом: {rc}", flush=True)

        # Пауза між прогонами (якщо не останній)
        if run_num < args.repeat:
            print(f"⏸️  Пауза {args.gap}s перед наступним прогоном...", flush=True)
            await asyncio.sleep(args.gap)
            print(
                f"▶️  Пауза завершена, переходимо до прогону {run_num + 1}", flush=True
            )

    print(f"\n{'='*60}", flush=True)
    print("🏁 УСІ ПРОГОНИ ЗАВЕРШЕНО", flush=True)
    print(f"📈 Фінальний exit код: {code}", flush=True)
    print("📊 Статистика:", flush=True)
    print(f"   ✅ Виконано прогонів: {args.repeat}", flush=True)
    # Якщо repeat=1, додатковий час пауз (gap) не додається
    print(
        f"   ⏱️  Загальний час: ~{args.repeat * args.duration + (args.repeat - 1) * args.gap}s",
        flush=True,
    )
    print(f"{'='*60}", flush=True)

    raise SystemExit(code)


if __name__ == "__main__":
    """Точка входу модуля.

    Обробляє KeyboardInterrupt та запускає головну async функцію.
    Гарантує коректний exit код навіть при Ctrl+C.
    """
    print("🌟 Запуск tools.run_window...", flush=True)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⌨️  Отримано KeyboardInterrupt, завершуємо...", flush=True)
        print("🛑 Програму зупинено користувачем", flush=True)
        # Використовуємо 'from None', щоб уникнути виводу traceback при завершенні по Ctrl+C
        raise SystemExit(EXIT_SIGINT) from None
    except Exception as e:
        print(f"\n❌ Неочікувана помилка: {e}", flush=True)
        raise SystemExit(1) from e
