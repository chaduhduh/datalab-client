"""
Provides various commonly used cross match functions
"""
from typing import List
from abc import ABC, abstractmethod
from .table import (
    XMatchTable,
    TableTypes
)
from .conversions import arcs_to_deg


class XMatchSearchType(ABC):
    """
    Abstract base class for standard xmatch search type definition
    """
    type_key = "xmatch_base"

    @abstractmethod
    def col_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None) -> str:
        """Generate the column SQL for a search type"""
        raise NotImplementedError("Search types must implement col_sql()")

    @abstractmethod
    def from_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None) -> str:
        """Generate the from SQL for a search type"""
        raise NotImplementedError("Search types must implement from_sql()")

    @abstractmethod
    def join_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None) -> str:
        """Generate the join SQL for a search type """
        raise NotImplementedError("Search types must implement join_sql()")

    @abstractmethod
    def where_sql(self, tables: List[XMatchTable]=None,
                  dl_table: XMatchTable=None) -> str:
        """Generate the where SQL for a search type"""
        raise NotImplementedError("Search types must implement where_sql()")

    @abstractmethod
    def sql(self, tables: List[XMatchTable]=None,
            dl_table: XMatchTable=None) -> str:
        """ Generate a full SQL query """
        raise NotImplementedError("Search types must implement sql()")

    def options(self) -> dict:
        """Returns the instances search options as a dict"""
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
                 dl_table: XMatchTable=None) -> str:
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
                 dl_table: XMatchTable=None) -> str:
        """ Generate SQL FROM clause """
        froms = []
        all_tables = tables if not dl_table else tables + [dl_table]
        for table in all_tables:
            froms.append(f"{table.name} as {table.alias()}")
        return ",\n".join(froms)

    def join_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None) -> str:
        """ Generate SQL JOIN clause """
        # by default we don't use join criteria
        return ""

    def sql(self, tables: List[XMatchTable]=None,
            dl_table: XMatchTable=None) -> str:
        """ Output full SQL for Q3C Query """
        output_cols = self.col_sql(
            tables=tables,
            dl_table=dl_table
        )
        from_clause = self.from_sql(
            tables=tables,
            dl_table=dl_table
        )
        join_clause = self.join_sql(
            tables=tables,
            dl_table=dl_table
        )
        where_clause = self.where_sql(
            tables=tables,
            dl_table=dl_table
        )

        return f'''
            SELECT
                {output_cols}
            FROM
                {from_clause}
                {join_clause}
            WHERE
                {where_clause}
        '''


class NearestNeighbor(_Q3CSearchBase):
    """
    Find the nearest neighbors in the given radius
    """
    type_key = "nearest_neighbor"

    def __init__(self, radius: float=5, exclude_non_matches: bool=False,
                 use_error_col: bool=None):
        self.radius = arcs_to_deg(radius)
        self.exclude_non_matches = exclude_non_matches
        self.use_error_col = use_error_col

    def join_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None) -> str:
        wheres = []
        match_t = dl_table if dl_table else tables[-1]
        targets = tables[:-1] if not dl_table and len(tables) > 1 else tables
        for table in targets:
            alias = table.alias()
            m_alias = f"{match_t.alias()}_sub_{alias}"
            j_type = "INNER JOIN" if self.exclude_non_matches else "LEFT JOIN"
            null_join = "ON true" if self.exclude_non_matches else "ON true"
            search_deg = self.radius
            if self.use_error_col:
                search_deg = table.error_circle
            wheres.append(f'''
                {j_type} LATERAL (
                    SELECT
                        {m_alias}.*
                    FROM
                        {match_t.name} AS {m_alias}
                    WHERE q3c_join(
                            {alias}.{table.ra}, {alias}.{table.dec},
                            {m_alias}.{match_t.ra},
                            {m_alias}.{match_t.dec},
                            {search_deg}
                        )
                    ORDER BY
                        q3c_dist(
                            {alias}.{table.ra}, {alias}.{table.dec},
                            {m_alias}.{match_t.ra},
                            {m_alias}.{match_t.dec}
                        )
                    ASC LIMIT 1
                ) as {match_t.alias()} {null_join}
            ''')
        return "\nAND\n".join(wheres)

    def from_sql(self, tables: List[XMatchTable]=None,
                 dl_table: XMatchTable=None) -> str:
        froms = []
        targets = tables[:-1] if not dl_table and len(tables) > 1 else tables
        for table in targets:
            froms.append(f"{table.name} as {table.alias()}")
        return ",\n".join(froms)

    def where_sql(self, tables: List[XMatchTable]=None,
                   dl_table: XMatchTable=None) -> str:
        # nearest neighbor relies an join so a WHERE clause isn't necessary
        return "1=1"

    def options(self):
        """Returns the instances search options as a dict"""
        return dict(
            radius=self.radius,
            exclude_non_matches=self.exclude_non_matches,
            use_error_col=self.use_error_col
        )


class AllMatches(_Q3CSearchBase):
    """
    Find all matches in the given spherical radius
    """
    type_key = "all_matches_spherical"

    def __init__(self, radius: float=5., use_error_circle: bool=False) -> None:
        super().__init__()
        self.radius = arcs_to_deg(radius)
        self.use_error_circle = use_error_circle

    def options(self) -> dict:
        return dict(
            radius=self.radius,
            use_error_circle=self.use_error_circle
            )

    def where_sql(self, tables: List[XMatchTable]=None,
                  dl_table: XMatchTable=None) -> str:
        wheres = []
        match_t = dl_table if dl_table else tables[-1]
        targets = tables[:-1] if not dl_table and len(tables) > 1 else tables
        for table in targets:
            alias = table.alias()
            m_alias = match_t.alias()
            search_deg = self.radius
            if self.use_error_circle:
                search_deg = f"{alias}.{table.error_circle}"
            wheres.append(f'''
                q3c_join(
                    {alias}.{table.ra}, {alias}.{table.dec},
                    {m_alias}.{match_t.ra}, {m_alias}.{match_t.dec},
                    {search_deg}
                )
            ''')
        return "\nAND\n".join(wheres)


class AllMatchesEllipse(_Q3CSearchBase):
    """
    Find all matches in the given ellipse based on the provided semi-major
    axis, axis ratio and position angle.
    """
    type_key = "all_matches_ellipse"

    def where_sql(self, tables: List[XMatchTable]=None,
                  dl_table: XMatchTable=None) -> str:
        wheres = []
        match_t = dl_table if dl_table else tables[-1]
        targets = tables[:-1] if not dl_table and len(tables) > 1 else tables
        for table in targets:
            alias = table.alias()
            m_alias = match_t.alias()
            wheres.append(f'''
                q3c_ellipse_join(
                    {alias}.{table.ra}, {alias}.{table.dec},
                    {m_alias}.{match_t.ra}, {m_alias}.{match_t.dec},
                    {alias}.{table.major_axis}, {alias}.{table.axis_ratio},
                    {alias}.{table.pos_angle}
                )
            ''')
        return "\nAND\n".join(wheres)
