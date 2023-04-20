# Built-in imports
import re
import string

# External imports
from pytest_cases import parametrize
from pytest_cases import parametrize_with_cases
from pytest_cases import get_case_id

# local imports
from constants import MODEL_NAME_REGEXP, REWARDED_DECISIONS_S3_KEY_REGEXP
from src.train.constants import SPECIAL_CHARACTERS_REGEXP, DIGITS_DT_REGEXP


def is_positive(x):
    return "positive" in get_case_id(x)


def is_negative(x):
    return "negative" in get_case_id(x)


class CasesS3Key:
    @parametrize(s3_key=[
        "rewarded_decisions/test-model-name-1.0/parquet/2021/01/06/20210106T000000Z-20210101T000000Z-10-e3e70682-c209-4cac-a29f-6fbed82c07cd.parquet",
    ])
    def case_positive(self, s3_key):
        return s3_key

    @parametrize(s3_key=[
        "rewarded_decisions//parquet/2021/01/06/20210106T000000Z-20210101T000000Z-10-e3e70682-c209-4cac-a29f-6fbed82c07cd.parquet",
        "/test-model-name/parquet/2021/01/06/20210106T000000Z-20210101T000000Z-10-e3e70682-c209-4cac-a29f-6fbed82c07cd.parquet",
        "rewarded_decisions/test-model-name-1.0/parquet/2021/01/06/20210106T000000-20210101T000000Z-10-e3e70682-c209-4cac-a29f-6fbed82c07cd.parquet",
        "rewarded_decisions/test-model-name-1.0/parquet/2021/01/06/20210106T000000Z-20210101T000000-10-e3e70682-c209-4cac-a29f-6fbed82c07cd.parquet",
        "rewarded_decisions/test-model-name-1.0/parquet/2021/01/06/20210106T000000Z-20210101T000000Z-10-e3e70682-c209-4cac-a29f-.parquet",
        "rewarded_decisions/test-model-name-1.0/parquet/2021/01/06/20210106T000000Z-20210101T000000Z-10-e3e70682-c209-4cac-a29f-6fbed82c07cd",
        "/rewarded_decisions/test-model-name-1.0/parquet/2021/01/06/20210106T000000Z-20210101T000000Z-10-e3e70682-c209-4cac-a29f-6fbed82c07cd.parquet"
    ])
    def case_negative(self, s3_key):
        return s3_key

@parametrize_with_cases("s3_key", cases=CasesS3Key, filter=is_positive)
def test_s3_key_regexp_is_matched(s3_key):
    assert re.match(REWARDED_DECISIONS_S3_KEY_REGEXP, s3_key) is not None

@parametrize_with_cases("s3_key", cases=CasesS3Key, filter=is_negative)
def test_s3_key_regexp_is_not_matched(s3_key):
    assert re.match(REWARDED_DECISIONS_S3_KEY_REGEXP, s3_key) is None



class CasesModelsNames:
    @parametrize(model_name=["messages-2.0", "appconfig"])
    def case_positive(self, model_name):
        return model_name

    @parametrize(model_name=["!", "a"*65, "-", "", " "])
    def case_negative(self, model_name):
        return model_name

@parametrize_with_cases("model_name", cases=CasesModelsNames, filter=is_positive)
def test_model_name_regexp_is_matched(model_name):
    assert re.match(MODEL_NAME_REGEXP, model_name) is not None

@parametrize_with_cases("model_name", cases=CasesModelsNames, filter=is_negative)
def test_model_name_regexp_is_not_matched(model_name):
    assert re.match(MODEL_NAME_REGEXP, model_name) is None



class CasesSpecialCharacters:
    invalid_chars = list(string.digits + string.ascii_letters + "-")

    @parametrize(s=list(set(string.printable).difference(set(invalid_chars))))
    def case_positive(self, s):
        return s

    @parametrize(s=invalid_chars)
    def case_negative(self, s):
        return s

@parametrize_with_cases("s", cases=CasesSpecialCharacters, filter=is_positive)
def test_special_characters_regexp_is_matched(s):
    assert re.match(SPECIAL_CHARACTERS_REGEXP, s) is not None

@parametrize_with_cases("s", cases=CasesSpecialCharacters, filter=is_negative)
def test_special_characters_regexp_is_not_matched(s):
    assert re.match(SPECIAL_CHARACTERS_REGEXP, s) is None



class CasesDigitsDatetime:
    valid_chars = [".", "-", ":", " "]

    @parametrize(s=valid_chars)
    def case_positive(self, s):
        return s

    @parametrize(s=list(set(string.printable).difference(set(valid_chars))))
    def case_negative(self, s):
        return s

@parametrize_with_cases("s", cases=CasesDigitsDatetime, filter=is_positive)
def test_digits_datetime_regexp_is_matched(s):
    assert re.match(DIGITS_DT_REGEXP, s) is not None

@parametrize_with_cases("s", cases=CasesDigitsDatetime, filter=is_negative)
def test_digits_datetime_regexp_is_not_matched(s):
    assert re.match(DIGITS_DT_REGEXP, s) is None



class CasesOrgAndProjectName:
    @parametrize(s=["a", "aa", "0", "0a", ])
    def case_positive(self, s):
        return s

    @parametrize(s=["!a", "-", "\a"])
    def case_negative(self, s):
        return s

@parametrize_with_cases("s", cases=CasesOrgAndProjectName, filter=is_positive)
def test_predeploy_org_and_project_name_regexp_is_matched(s):
    regexp = '^[a-z0-9]+$'
    assert re.match(regexp, s) is not None

@parametrize_with_cases("s", cases=CasesOrgAndProjectName, filter=is_negative)
def test_predeploy_org_and_project_name_regexp_is_not_matched(s):
    regexp = '^[a-z0-9]+$'
    assert re.match(regexp, s) is None