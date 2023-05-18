"""
Provides various commonly used cross match functions
"""
from typing import List
from abc import ABC, abstractmethod
from .table import (
    XMatchTable,
    TableTypes
)


class XMatchSearchType(ABC):
    """
    Abstract base class for standard xmatch search type definition
    """
    type_key = "xmatch_base"

    @abstractmethod
    def col_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None, degrees=0.0) -> str:
        """Generate the column SQL for a search type"""
        raise NotImplementedError("Search types must implement col_sql()")

    @abstractmethod
    def from_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None, degrees=0.0) -> str:
        """Generate the from SQL for a search type"""
        raise NotImplementedError("Search types must implement from_sql()")

    @abstractmethod
    def join_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None, degrees=0.0) -> str:
        """Generate the join SQL for a search type """
        raise NotImplementedError("Search types must implement join_sql()")

    @abstractmethod
    def where_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None, degrees=0.0) -> str:
        """Generate the where SQL for a search type"""
        raise NotImplementedError("Search types must implement where_sql()")

    def request_format(self) -> str:
        """Returns the search options for this type instance as dict"""
        return {}


class _Q3CSearchBase(XMatchSearchType):
    """
    Our base q3c search definitions
    """
    type_key = "q3c_base"

    @staticmethod
    def q3c_dist_cols(tables: List[XMatchTable]=None,
                      dl_table: XMatchTable=None):
        """
        Generate the q3c dist output column
        """
        dist_cols = []
        match_table = dl_table if dl_table else tables[-1]
        targets = tables[:-1] if not dl_table and len(tables) > 1 else tables
        for table in targets:
            alias = table.alias()
            m_alias = match_table.alias()
            dist_cols.append(f'''(q3c_dist(
                    {alias}.{table.ra}, {alias}.{table.dec},
                    {m_alias}.{match_table.ra}, {m_alias}.{match_table.dec}
                )*3600.0) as {alias}_dist_arcsec''')
        return ",\n".join(dist_cols)

    def col_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None, degrees=0.0) -> str:
        """
        Given a set of user tables and DL table generate the appropriate
        column SQL
        """
        output_cols = []
        all_tables = tables if not dl_table else tables + [dl_table]
        for table in all_tables:
            alias = table.alias()
            for col in table.output_cols:
                col_name = f"{alias}_{col}"
                # for Data Lab tables just output the regular col name
                if table.table_type == TableTypes.TAPDB:
                    col_name = col
                col_str = f"{alias}.{col} as {col_name}"
                col_str = f"{alias}.*" if col == "all" else col_str
                output_cols.append(col_str)

        output_cols.append(
            self.q3c_dist_cols(tables, dl_table))
        return ",\n".join(output_cols)

    def from_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None, degrees=0.0) -> str:
        """ Generate SQL FROM clause """
        froms = []
        all_tables = tables if not dl_table else tables + [dl_table]
        for table in all_tables:
            froms.append(f"{table.name} as {table.alias()}")
        return ",\n".join(froms)

    def join_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None, degrees=0.0) -> str:
        """ Generate SQL JOIN clause """
        # by default we don't use join criteria
        return ""


class NearestNeighbor(_Q3CSearchBase):
    """
    Run a nearest neighbor type of cross match
    """
    type_key = "nearest_neighbor"

    def __init__(self, exclude_non_matches: bool=False):
        self.exclude_non_matches = exclude_non_matches

    def join_sql(self, tables: List[XMatchTable]=None,
                   dl_table: XMatchTable=None, degrees=0.0) -> str:
        wheres = []
        match_t = dl_table if dl_table else tables[-1]
        targets = tables[:-1] if not dl_table and len(tables) > 1 else tables
        for table in targets:
            alias = table.alias()
            m_alias = f"{match_t.alias()}_sub_{alias}"
            j_type = "INNER" if self.exclude_non_matches else "LEFT"
            wheres.append(f'''
                {j_type} JOIN LATERAL (
                    SELECT
                        {m_alias}.*
                    FROM
                        {match_t.name} AS {m_alias}
                    WHERE q3c_join(
                            {alias}.{table.ra}, {alias}.{table.dec},
                            {m_alias}.{match_t.ra},
                            {m_alias}.{match_t.dec},
                            {degrees}
                        )
                    ORDER BY
                        q3c_dist(
                            {alias}.{table.ra}, {alias}.{table.dec},
                            {m_alias}.{match_t.ra},
                            {m_alias}.{match_t.dec}
                        )
                    ASC LIMIT 1
                ) as {match_t.alias()} ON true = true
            ''')
            # TODO: this "on true" part was carried over from other code but
            # might not be working as intended. We might need to remove the on
            # true part when we "exclude_non_matches" is set to true
        return "\nAND\n".join(wheres)

    def from_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None, degrees=0.0) -> str:
        froms = []
        for table in tables:
            froms.append(f"{table.name} as {table.alias()}")
        return ",\n".join(froms)

    def where_sql(self, tables: List[XMatchTable]=None,
                   dl_table: XMatchTable=None, degrees=0.0) -> str:
        # nearest neighbor relies an join so a WHERE clause isn't necessary
        return "1=1"

    def request_format(self) -> str:
        return dict(
            exclude_non_matches=True
        )


class AllMatches(_Q3CSearchBase):
    """
    Find all matches in the given radius
    """
    type_key = "all_matches"

    def where_sql(self, tables: List[XMatchTable]=None,
                   dl_table: XMatchTable=None, degrees=0.0) -> str:
        wheres = []
        match_t = dl_table if dl_table else tables[-1]
        targets = tables[:-1] if not dl_table and len(tables) > 1 else tables
        for table in targets:
            alias = table.alias()
            m_alias = match_t.alias()
            wheres.append(f'''
                q3c_join(
                    {alias}.{table.ra}, {alias}.{table.dec},
                    {m_alias}.{match_t.ra}, {m_alias}.{match_t.dec},
                    {degrees}
                )
            ''')
        return "\nAND\n".join(wheres)
