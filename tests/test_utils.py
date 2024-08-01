import pytest
from xqute.utils import replace_with_leading_space


@pytest.mark.parametrize(
    "input_str, old_str, new_str, expected_output",
    [
        ("a\nb\nc", "b", "x", "a\nx\nc"),
        ("a\nb\nc", "b", "x\ny", "a\nx\ny\nc"),
        ("a\n b\nc", "b", "x", "a\n x\nc"),
        ("a\n b\nc", "b", "x\ny\nz", "a\n x\n y\n z\nc"),
        ("a\n b\nc", "b", "x\n\nz", "a\n x\n\n z\nc"),
    ],
)
def test_replace_with_leading_space(input_str, old_str, new_str, expected_output):
    assert replace_with_leading_space(input_str, old_str, new_str) == expected_output
