
import os
import sys
from pathlib import Path

# Make sure python is looking in the correct spot for the source code
src_path = os.path.join(Path(__file__).parents[1], "src")
sys.path.append(src_path)

# Import my source code
from list_inspector import remove_string_elements


def test_remove_string_elements_1():


    my_list = [123, 456, "abc", 789,]

    new_list = remove_string_elements(my_list)

    expected_list = [123, 456, 789]

    assert new_list == expected_list

def test_remove_string_elements_2():


    my_list = ["123", "456", "abc", "789",]

    new_list = remove_string_elements(my_list)

    expected_list = []

    assert new_list == expected_list


