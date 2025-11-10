-- todo dictGet?

create function parse_sb32 as (enc) -> arrayFold(
    acc,x -> bitShiftLeft(acc,5)+x,
    arrayMap(
        char -> toUInt64(indexOf(ngrams('234567abcdefghijklmnopqrstuvwxyz', 1), char)-1),
        ngrams(enc,1)
    ),
    toUInt64(0)
)