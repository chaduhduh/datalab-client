"""
Provides various HTTP tools for processing cross match requests
"""
import json
from typing import List
from .table import XMatchTable
from .search_types import (
    XMatchSearchType,
    AllMatches
)


class XMatchRequest:
    """
    various REST related utility functions for the XMatch
    """
    @staticmethod
    def is_valid_xm_table(table_data=None):
        """
        Validate a XMTable request object
        """
        if not table_data:
            return None, "no table was provided"
        table_name = table_data.get("table_name", "")
        msgs = []
        is_valid = True
        if len(table_name) == 0:
            is_valid = False
            msgs.append("The 'table_name' key is required for an xmatch table"+
                        ". Received "+json.dumps(table_data, skipkeys=True)
                        )
        return is_valid, ", ".join(msgs)

    @staticmethod
    def validate_payload(xm_data: dict=None):
        """
        Validate an xmatch request payload
        """
        if not xm_data:
            return False, "request body must be valid JSON"
        # make the data easier to work with

        user_tables = xm_data.get('tables', [])
        dl_table = xm_data.get('dl_table', None)
        all_tables = user_tables if not dl_table else user_tables + [dl_table]
        # start validating things

        if len(user_tables) == 0:
            return False, ("The 'tables' key is required and must contain"
                           " at least one mydb table")
        if len(user_tables) == 1 and 'dl_table' not in xm_data:
            return False, ("The 'dl_table' key is required when only one "
                                    "mydb table is provided.")
        for table in all_tables:
            valid_table, msg = XMatchRequest.is_valid_xm_table(table)
            if not valid_table:
                return False, msg

        return True, "Success"

    @staticmethod
    def format(tables: List[XMatchTable]=None, dl_table: XMatchTable=None,
                  search_type: XMatchSearchType=AllMatches(),
                  radius: float=5, async_: bool=True,
                  output_options: dict=None):
        """
        Returns XMatch objects in request format
        """
        data = dict(
            tables=[],
            dl_table=None,
            radius=radius,
            search_type="",
            search_options=dict(),
            async_=async_,
            output_options=output_options
        )

        if tables:
            data["tables"] += [ t.request_format() for t in tables ]

        if dl_table:
            data["dl_table"] = dl_table.request_format()

        data["search_type"] = search_type.type_key
        data["search_options"] = search_type.request_format()

        return data


class XMatchResponse:
    """
    Various XMatch related standard response tools
    """
    @staticmethod
    def json(message: str="", data: dict=None):
        """
        Standard Xmatch response format
        """
        return json.dumps(dict(
            message=message,
            data=data
            ))