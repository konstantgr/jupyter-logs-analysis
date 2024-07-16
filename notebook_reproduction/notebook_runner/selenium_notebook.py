import logging
from enum import auto
from pathlib import Path
from time import sleep
from typing import Optional, Tuple

import pyperclip
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from strenum import UppercaseStrEnum

from notebook_reproduction.notebook_runner import (
    CELL_EDIT_AREA,
    CELL_ELEMENT,
    CELL_OUTPUT_AREA,
    INSERT_CELL_BELOW_ELEMENT,
    INTERRUPT_KERNEL_SELECTOR,
    PROGRESS_EXECUTION_ELEMENT,
    TRACEBACK_MESSAGE,
)

log = logging.getLogger(__name__)


class LoggingFlag(UppercaseStrEnum):
    START_SESSION = auto()
    FINISH_SESSION = auto()
    ACTION = auto()


def get_string_size_in_mbs(s: str) -> float:
    return len(s.encode("utf-8")) / 1e6


class SeleniumNotebook:
    def __init__(
        self,
        notebook_path: Path,
        driver_path: Path,
        server: Path,
        sep: str = "\n#%% --\n",
        headless: bool = False,
    ):
        self.notebook_path = notebook_path
        self.driver_path = driver_path
        self.server = server
        self.sep = sep
        self.headless = headless

        self.driver = None

        self.connection_file = None
        self.blocking_client = None

    @property
    def cells(self) -> list[WebElement]:
        return self.driver.find_elements(By.XPATH, CELL_ELEMENT)

    @property
    def last_cell(self) -> WebElement:
        return self[-1]

    @staticmethod
    def get_cell_output(cell: WebElement) -> str:
        cell_output: str = ""
        try:
            all_cell_outputs = cell.find_elements(By.XPATH, CELL_OUTPUT_AREA)
            cell_output = "".join([output.text for output in all_cell_outputs])
        except Exception as e:
            print("Error while printing cell output:", str(e))
        finally:
            return cell_output

    def as_json(self):
        return [{"text": self.get_cell_source(cell)} for cell in self.cells]

    def _wait_for_execution(
        self,
        cell: WebElement,
        waiting_time_delta: float = 0.5,
        max_waiting_time: float = 4,
    ) -> None:
        wait_time = 0
        while True:
            try:
                # Get [*] (progress) an element. If it's still "*" -- waiting
                # If no element it will produce error
                cell.find_element(
                    By.XPATH,
                    PROGRESS_EXECUTION_ELEMENT,
                )
                sleep(waiting_time_delta)
                wait_time += waiting_time_delta

                # If we are already waiting for too long -- interrupting kernel
                # TODO: check output/memory instead of time,
                #  because there may be models training, etc
                if wait_time > max_waiting_time:
                    output = self.get_cell_output(cell)
                    if get_string_size_in_mbs(output) > 10:
                        self.interrupt_kernel()

            except NoSuchElementException:
                # No [*] element found, so the cell finished executing
                break
            except Exception as e:
                # There could be many different errors, such as errors with memory,
                # caused by long outputs, or any runtime errors, etc.
                # so I keep a general exception type here
                print(type(e).__name__, e)
                break

    def execute_cell(self, cell_num: int, with_create: bool = False) -> Tuple[bool, str]:
        # Make active the cell, which should be executed
        cell = self[cell_num]
        cell.click()

        # Executed it, but pressing the hotkey
        action = ActionChains(self.driver).key_down(Keys.COMMAND).key_down(Keys.ENTER)
        action.perform()

        # Wait for cell execution
        self._wait_for_execution(cell)
        if with_create and cell == self.last_cell:
            self.add_cell(cell_num=-1)

        # After cell execution, retrieve cell output
        # and check whether it error or not
        output = self.get_cell_output(cell)
        error = True if TRACEBACK_MESSAGE in output else False

        return error, output

    def add_cell(self, cell_num: Optional[int] = None, sleep_time: float = 0.5) -> WebElement:
        # Find cell after which new cell should be added
        cell = self[cell_num] if cell_num is not None else self.last_cell
        cell.click()

        # Then, press button to create new cell
        self.driver.find_element(By.XPATH, INSERT_CELL_BELOW_ELEMENT).click()

        log.info("[ACTION] CREATE CELL")
        sleep(sleep_time)

        return cell

    def execute_all(self, sleep_time: float = 0.5) -> Tuple[bool, Optional[int]]:
        for num, cell in enumerate(self.cells):
            log.info(f"[ACTION] EXECUTE CELL {num}")

            error, output = self.execute_cell(cell)
            sleep(sleep_time)

            # If error appeared, no more cells will be executed, so return the error
            if error:
                log.info(f"[ERROR] Cell with num {num} caused an error".upper())
                return False, num

        # If error doesn't appear, everything is ok and nothing to return
        log.info("[NO ERRORS]")
        return True, None

    def change_cell(
        self,
        cell_num: int,
        new_cell_content: str,
        sleep_time: float = 0.5,
    ) -> WebElement:
        cell = self[cell_num]
        edit_area = cell.find_element(By.XPATH, CELL_EDIT_AREA)

        # Delete previous code
        actions = ActionChains(self.driver)
        actions.move_to_element(edit_area).click(edit_area)
        actions.key_down(Keys.COMMAND).send_keys("a").key_up(Keys.COMMAND).send_keys(Keys.BACKSPACE)
        actions.perform()

        # Copy new source code
        pyperclip.copy(new_cell_content)

        # Paste new source code
        actions = ActionChains(self.driver)
        actions.move_to_element(edit_area).click(edit_area)
        actions.key_down(Keys.COMMAND).send_keys("v")
        actions.perform()

        log.info(f"[ACTION] CHANGE SOURCE OF CELL {cell_num}")
        sleep(sleep_time)
        return cell

    def restart_kernel(self) -> None:
        ZERO_KEY: str = "0"
        actions = (
            ActionChains(self.driver)
            .send_keys(Keys.ESCAPE)
            .send_keys(ZERO_KEY)
            .send_keys(ZERO_KEY)
            .send_keys(Keys.RETURN)
        )
        actions.perform()
        log.info("[ACTION] RESTART KERNEL")

    def interrupt_kernel(self) -> None:
        # Find interrupt kernel button and click it
        button = self.driver.find_element(By.CSS_SELECTOR, INTERRUPT_KERNEL_SELECTOR)
        button.click()

        log.info("[ACTION] INTERRUPT KERNEL")

    @staticmethod
    def get_cell_source(cell: WebElement) -> str:
        code_mirror_area = cell.find_element(By.XPATH, CELL_EDIT_AREA)
        return code_mirror_area.text

    # def __get_connection_file(self) -> str:
    #     cell = self.change_cell(0, "import ipykernel; print(ipykernel.get_connection_file())")
    #     connection_file = self.get_cell_output(cell)
    #     return connection_file

    def __enter__(self):
        log.info("[START_SESSION] START")

        sleep_time: float = 5.0
        notebook_path: str = str(self.server / "notebooks/" / self.notebook_path)

        service = Service(executable_path=str(self.driver_path))
        options = webdriver.ChromeOptions()

        # Sleep before webdriver starts
        sleep(sleep_time)

        if self.headless:
            options.add_argument("headless")
        self.driver = webdriver.Chrome(service=service, options=options)

        print(f"Notebook path is {notebook_path}")
        self.driver.get(notebook_path)

        # Sleep to start the webpage
        sleep(10)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        log.info("[FINISH_SESSION] FINISH")
        self.driver.quit()

    def __getitem__(self, cell_num: int) -> WebElement:
        if cell_num >= len(self.cells):
            raise IndexError(f"Unexpected index number in list with size {len(self.cells)}")

        return self.cells[cell_num]

    def __str__(self) -> str:
        return self.sep.join([self.get_cell_source(cell) for cell in self.cells])
