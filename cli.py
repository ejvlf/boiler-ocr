import json
import cv2
import argparse
import pytesseract
import logging
import signal
from datetime import datetime
import time
import os

from objects.analytics import ReportProcessor
from objects.applications import form_database_connection, form_logger, get_settings
from objects.boiler import BoilerData
from persistence.database import MariaDBHandler

from flask import Flask, request, jsonify


"""

Aplicação gloriosa que lê o leitor das caldeiras Ferlux e 
transforma a informação do visor em extraordinarios dados. 

"""

def handle_sigterm():
    raise KeyboardInterrupt

def reference_command(args):
    # logger
    main_logger = form_logger(args.debug, False, "reference")
    
    # definições
    app_settings = get_settings(f"{args.settings}.json")

    database_url = form_database_connection(app_settings["app"]["database"]["user"],
                                            app_settings["app"]["database"]["password"],
                                            app_settings["app"]["database"]["host"],
                                            app_settings["app"]["database"]["database"]
                                            )

    # base de dados
    db_handler = MariaDBHandler(database_url, main_logger)

    # Os dados que existem sem informação de consumo adicional
    report_ids_to_proc = db_handler.get_partial_reports()

    if report_ids_to_proc is None or len(report_ids_to_proc) == 0:
        main_logger.info("No reports found that require reference data. Exiting.")
        return

    for report_id in report_ids_to_proc:
        try:
            print(f"Para o relatório ID {report_id[0]} que vai de {report_id[1]} a {report_id[2]}")
            reference_consumption = input("Sacos de pellets consumidos: ")
            reference_temperature = int(input("Temperatura de referência para o consumo: "))
            reference_sensor = float(input("Temperatura de referência para o sensor: "))

            data = {
                "id": int(datetime.now().timestamp()),
                "report_id": report_id[0],
                "quantity": reference_consumption,
                "max_boiler_temperature": reference_temperature,
                "max_room_temperature": reference_sensor
                }
            
            db_handler.insert_consumption_record(data)

            print("Para proseguir carrega no ENTER. Para sair, carrega CTRL+C")
            input()

        except ValueError:
            main_logger.warning("Valor inválido")

        except Exception as e:
            main_logger.error(f"Falhou a persistir: {e}")
            break
        except KeyboardInterrupt:
            main_logger.info("Fim do programa")
            break
    main_logger.info("Todos os relatórios processados")

def report_command(args):

    # logger
    main_logger = form_logger(args.debug, args.file_log, "report")
    
    # definições
    app_settings = get_settings(f"{args.settings}.json")

    database_url = form_database_connection(app_settings["app"]["database"]["user"],
                                            app_settings["app"]["database"]["password"],
                                            app_settings["app"]["database"]["host"],
                                            app_settings["app"]["database"]["database"]
                                            )

    # base de dados
    db_handler = MariaDBHandler(database_url, main_logger)

    records = db_handler.get_report_records_after()
    main_logger.info(f"Records fetched: {len(records)}")

    report_records = ReportProcessor(main_logger)
    records_to_persist = report_records.process_report_data(records)

    for record in records_to_persist:
        main_logger.info(f"Persisting report record with start time {record.start_time} and end time {record.end_time}")
        db_handler.insert_report_record(record)

    main_logger.info("Finished processing report data")

def main():
    parser = argparse.ArgumentParser(
        description="Ferlux Boiler OCR System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s run --settings config --debug
  %(prog)s report --settings config --file-log
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands', required=True)

    # Report subcommand
    report_parser = subparsers.add_parser('report', help='Generate analytics report from collected data')
    report_parser.add_argument("--debug", help="Enable debug logging", action="store_true")
    report_parser.add_argument("--file-log", help="Log to file instead of console", action="store_true")
    report_parser.add_argument("--dry-run", help="Run without persisting to database", action="store_true")
    report_parser.add_argument("--settings", help="Settings file name (without .json)", required=True)

    # Reference subcommand
    reference_parser = subparsers.add_parser('reference', help='Input reference data for the reports')
    reference_parser.add_argument("--debug", help="Enable debug logging", action="store_true")
    reference_parser.add_argument("--dry-run", help="Run without persisting to database", action="store_true")
    reference_parser.add_argument("--settings", help="Settings file name (without .json)", required=True)

    args = parser.parse_args()

    if args.command == 'report':
        report_command(args)
    elif args.command == 'reference':
        reference_command(args)

if __name__ == "__main__":
    main()