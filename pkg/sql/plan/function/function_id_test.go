// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// all fixed function ids defined at 2024-12-12
var predefinedFunids = map[int]int{
	EQUAL:              0,
	NOT_EQUAL:          1,
	GREAT_THAN:         2,
	GREAT_EQUAL:        3,
	LESS_THAN:          4,
	LESS_EQUAL:         5,
	BETWEEN:            6,
	UNARY_PLUS:         7,
	UNARY_MINUS:        8,
	UNARY_TILDE:        9,
	PLUS:               10,
	MINUS:              11,
	MULTI:              12,
	DIV:                13,
	INTEGER_DIV:        14,
	MOD:                15,
	CONCAT:             16,
	AND:                17,
	OR:                 18,
	XOR:                19,
	NOT:                20,
	CAST:               21,
	BIT_CAST:           22,
	IS:                 23,
	ISNOT:              24,
	ISNULL:             25,
	ISNOTNULL:          26,
	ISTRUE:             27,
	ISNOTTRUE:          28,
	ISFALSE:            29,
	ISNOTFALSE:         30,
	ISEMPTY:            31,
	NOT_IN_ROWS:        32,
	OP_BIT_AND:         33,
	OP_BIT_OR:          34,
	OP_BIT_XOR:         35,
	OP_BIT_SHIFT_LEFT:  36,
	OP_BIT_SHIFT_RIGHT: 37,

	ABS:               38,
	ACOS:              39,
	ADDDATE:           40,
	ADDTIME:           41,
	AES_DECRYPT:       42,
	AES_ENCRYPT:       43,
	ANY_VALUE:         44,
	APPROX_COUNT:      45,
	ARRAY_AGG:         46,
	ARRAY_APPEND:      47,
	ARRAY_CAT:         48,
	ARRAY_CONTAINS:    49,
	ARRAY_POSITION:    50,
	ARRAY_SIZE:        51,
	ASCII:             52,
	ASIN:              53,
	ASSERT:            54,
	ATAN:              55,
	ATAN2:             56,
	AVG:               57,
	AVG_TW_CACHE:      58,
	AVG_TW_RESULT:     59,
	BASE64_DECODE:     60,
	BASE64_ENCODE:     61,
	BIT_AND:           62,
	BIT_LENGTH:        63,
	BIT_NOT:           64,
	BIT_OR:            65,
	BIT_XOR:           66,
	BITAGG_AND:        67,
	BITAGG_OR:         68,
	BOOLAGG_AND:       69,
	BOOLAGG_OR:        70,
	CASE:              71,
	CEIL:              72,
	CHR:               73,
	COALESCE:          74,
	FIELD:             75,
	CONCAT_WS:         76,
	CONTAINS:          77,
	CORR:              78,
	COS:               79,
	COT:               80,
	CRC32:             81,
	COUNT:             82,
	COUNT_IF:          83,
	COVAR_POP:         84,
	COVAR_SAMPLE:      85,
	CONVERT_TZ:        86,
	CUME_DIST:         87,
	CURRENT_DATE:      88,
	CURRENT_TIMESTAMP: 89,
	DATE_FROM_PARTS:   90,
	DATE_PART:         91,
	DATEADD:           92,
	DATEDIFF:          93,
	TIMEDIFF:          94,
	TIMESTAMPDIFF:     95,
	DENSE_RANK:        96,
	MO_WIN_DIVISOR:    97,
	EMPTY:             98,
	ENDSWITH:          99,
	EXP:               100,
	FINDINSET:         101,
	FIRST_VALUE:       102,
	FLOOR:             103,
	GREATEST:          104,
	GROUPING:          105,
	HASH:              106,
	HASH_AGG:          107,
	HEX_DECODE:        108,
	HEX_ENCODE:        109,
	HEX:               110,
	UNHEX:             111,
	MD5:               112,
	IFF:               113,
	IFNULL:            114,
	ILIKE:             115,
	ILIKE_ALL:         116,
	ILIKE_ANY:         117,
	IN:                118,
	LAG:               119,
	LAST_VALUE:        120,
	LEAD:              121,
	LEAST:             122,
	LEFT:              123,
	LENGTH:            124,
	LENGTH_UTF8:       125,
	LIKE:              126,
	LIKE_ALL:          127,
	LIKE_ANY:          128,
	LN:                129,
	NOT_IN:            130,
	LOG:               131,
	LOG2:              132,
	LOG10:             133,
	LOWER:             134,
	LPAD:              135,
	LTRIM:             136,
	MAX:               137,
	MEDIAN:            138,
	MIN:               139,
	MODE:              140,
	MONTH:             141,
	NORMAL:            142,
	NTH_VALUE:         143,
	NTILE:             144,
	NULLIF:            145,
	PERCENT_RANK:      146,
	PI:                147,
	POSITION:          148,
	POW:               149,
	PREFIX_EQ:         150,
	PREFIX_IN:         151,
	PREFIX_BETWEEN:    152,
	RADIAN:            153,
	RANDOM:            154,
	RANK:              155,
	REGEXP:            156,
	REGEXP_INSTR:      157,
	REGEXP_LIKE:       158,
	REGEXP_REPLACE:    159,
	REGEXP_SUBSTR:     160,
	REG_MATCH:         161,
	NOT_REG_MATCH:     162,
	REPEAT:            163,
	REPLACE:           164,
	REVERSE:           165,
	RIGHT:             166,
	ROUND:             167,
	ROW_NUMBER:        168,
	RPAD:              169,
	RTRIM:             170,
	SIGN:              171,
	SIN:               172,
	SINH:              173,
	SPACE:             174,
	SPLIT:             175,
	SPLIT_PART:        176,
	SQRT:              177,
	STARCOUNT:         178,
	STARTSWITH:        179,
	STDDEV_POP:        180,
	STDDEV_SAMPLE:     181,
	SUBSTR:            182,
	SUM:               183,
	SYSDATE:           184,
	GROUP_CONCAT:      185,
	TAN:               186,
	TO_DATE:           187,
	STR_TO_DATE:       188,
	TO_INTERVAL:       189,
	TRANSLATE:         190,
	TRIM:              191,
	UNIFORM:           192,
	SHA1:              193,
	SHA2:              194,
	UTC_TIMESTAMP:     195,
	UNIX_TIMESTAMP:    196,
	FROM_UNIXTIME:     197,
	UPPER:             198,
	VAR_POP:           199,
	VAR_SAMPLE:        200,
	LAST_DAY:          201,
	MAKEDATE:          202,
	DATE:              203,
	TIME:              204,
	DAY:               205,
	DAYOFYEAR:         206,
	INTERVAL:          207,
	EXTRACT:           208,
	OCT:               209,
	SUBSTRING:         210,
	ENCODE:            211,
	DECODE:            212,
	TO_BASE64:         213,
	FROM_BASE64:       214,
	SUBSTRING_INDEX:   215,
	WEEK:              216,
	WEEKDAY:           217,
	YEAR:              218,
	HOUR:              219,
	MINUTE:            220,
	SECOND:            221,
	TO_DAYS:           222,
	TO_SECONDS:        223,

	DATE_ADD:              224,
	DATE_SUB:              225,
	APPROX_COUNT_DISTINCT: 226,

	LOAD_FILE:            227,
	SAVE_FILE:            228,
	DATABASE:             229,
	USER:                 230,
	CONNECTION_ID:        231,
	CHARSET:              232,
	CONVERT:              233,
	CURRENT_ROLE:         234,
	FOUND_ROWS:           235,
	ICULIBVERSION:        236,
	LAST_INSERT_ID:       237,
	LAST_QUERY_ID:        238,
	LAST_UUID:            239,
	ROLES_GRAPHML:        240,
	ROW_COUNT:            241,
	VERSION:              242,
	COLLATION:            243,
	CURRENT_ACCOUNT_ID:   244,
	CURRENT_ACCOUNT_NAME: 245,
	CURRENT_ROLE_ID:      246,
	CURRENT_ROLE_NAME:    247,
	CURRENT_USER_ID:      248,
	CURRENT_USER_NAME:    249,

	TIMESTAMP:            250,
	DATE_FORMAT:          251,
	JSON_EXTRACT:         252,
	JSON_EXTRACT_STRING:  253,
	JSON_EXTRACT_FLOAT64: 254,
	JSON_QUOTE:           255,
	JSON_UNQUOTE:         256,
	JSON_ROW:             257,

	JQ:       258,
	TRY_JQ:   259,
	WASM:     260,
	TRY_WASM: 261,
	FORMAT:   262,
	SLEEP:    263,
	INSTR:    264,
	LOCATE:   265,

	UUID:           266,
	SERIAL:         267,
	SERIAL_FULL:    268,
	SERIAL_EXTRACT: 269,
	BIN:            270,

	ENABLE_FAULT_INJECTION:  271,
	DISABLE_FAULT_INJECTION: 272,
	ADD_FAULT_POINT:         273,
	REMOVE_FAULT_POINT:      274,
	TRIGGER_FAULT_POINT:     275,
	MO_WIN_TRUNCATE:         276,

	MO_MEMORY_USAGE:                277,
	MO_ENABLE_MEMORY_USAGE_DETAIL:  278,
	MO_DISABLE_MEMORY_USAGE_DETAIL: 279,

	MO_CTL: 280,

	MO_SHOW_VISIBLE_BIN:      281,
	MO_SHOW_VISIBLE_BIN_ENUM: 282,
	MO_SHOW_COL_UNIQUE:       283,

	MO_TABLE_ROWS:    284,
	MO_TABLE_SIZE:    285,
	MO_TABLE_COL_MAX: 286,
	MO_TABLE_COL_MIN: 287,

	MO_LOG_DATE:    288,
	PURGE_LOG:      289,
	MO_ADMIN_NAME:  290,
	MO_CU:          291,
	MO_CU_V1:       292,
	MO_EXPLAIN_PHY: 293,

	GIT_VERSION:   294,
	BUILD_VERSION: 295,

	VALUES:                        296,
	BINARY:                        297,
	INTERNAL_CHAR_LENGTH:          298,
	INTERNAL_CHAR_SIZE:            299,
	INTERNAL_NUMERIC_PRECISION:    300,
	INTERNAL_NUMERIC_SCALE:        301,
	INTERNAL_DATETIME_SCALE:       302,
	INTERNAL_COLUMN_CHARACTER_SET: 303,
	INTERNAL_AUTO_INCREMENT:       304,

	CAST_INDEX_TO_VALUE:       305,
	CAST_VALUE_TO_INDEX:       306,
	CAST_INDEX_VALUE_TO_INDEX: 307,

	CAST_NANO_TO_TIMESTAMP: 308,
	CAST_RANGE_VALUE_UNIT:  309,

	NEXTVAL: 310,
	SETVAL:  311,
	CURRVAL: 312,
	LASTVAL: 313,

	SUMMATION:         314,
	L1_NORM:           315,
	L2_NORM:           316,
	INNER_PRODUCT:     317,
	COSINE_SIMILARITY: 318,
	VECTOR_DIMS:       319,
	NORMALIZE_L2:      320,
	L2_DISTANCE:       321,
	L2_DISTANCE_SQ:    322,
	COSINE_DISTANCE:   323,
	CLUSTER_CENTERS:   324,
	SUB_VECTOR:        325,

	PYTHON_UDF: 326,

	MO_CPU:      327,
	MO_MEMORY:   328,
	MO_CPU_DUMP: 329,

	BITMAP_BIT_POSITION:  330,
	BITMAP_BUCKET_NUMBER: 331,
	BITMAP_COUNT:         332,
	BITMAP_CONSTRUCT_AGG: 333,
	BITMAP_OR_AGG:        334,

	FULLTEXT_MATCH:       335,
	FULLTEXT_MATCH_SCORE: 336,

	FUNCTION_END_NUMBER: 337,
}

func Test_funids(t *testing.T) {
	check := func(fid int) {
		if _, ok := predefinedFunids[fid]; !ok {
			require.Failf(t, "no functionid in 'predefinedFunids'", "put funtion id %d into 'predefinedFunids'", fid)
		} else {
			require.Equal(t, fid, predefinedFunids[fid])
		}
		require.Greater(t, FUNCTION_END_NUMBER, fid)
	}
	for _, fun := range allSupportedFunctions {
		check(fun.functionId)
	}

	for _, fid := range functionIdRegister {
		check(int(fid))
	}
}
