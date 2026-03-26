import hashlib


def count_zeroes(bytes):
    zero_count = 0
    for byte in bytes:
        for bit in range(7, -1, -1):
            if (byte >> bit & 1) != 0:
                return zero_count
            zero_count += 1
    return zero_count


def mst_dfs(tree, visited, start, target_k, target_v, levels=0):
    if target_v is None:
        return True, None, None, None, levels
    
    if isinstance(target_k, str):
        target_k = target_k.encode()
        target_k = (target_k, count_zeroes(hashlib.sha256(target_k).digest()) // 2)

    if start is None:
        return False, None, None, None, levels

    if start in visited:
        raise Exception(f"MST cycle detected: node={start}")

    visited.add(start)

    if start not in tree:
        return False, None, None, None, levels - 1

    node = tree[start]

    left_state = mst_dfs(tree, visited, node["l"], target_k, target_v, levels + 1)

    key = b""
    found = left_state[0]
    depth = None
    first_key = None
    last_key = left_state[2]
    max_levels = left_state[4]

    for entry in node["e"]:
        key = key[: entry["p"]] + entry["k"]

        if first_key is None:
            first_key = key

        if last_key is not None and last_key >= key:
            raise Exception(f"MST key not greater than previous: key={key} last_key={last_key} node={start}")

        right_state = mst_dfs(tree, visited, entry["t"], target_k, target_v, levels + 1)

        max_levels = max(max_levels, right_state[4])

        if right_state[1] is not None and right_state[1] <= key:
            raise Exception(f"MST right subtree first key not greater than parent: right_first={right_state[1]} key={key} node={start}")

        found = found or right_state[0]

        this_depth = count_zeroes(hashlib.sha256(key).digest()) // 2
        if depth is None:
            depth = this_depth
        if depth != this_depth:
            raise Exception(f"MST node has entries with different depths: depth={this_depth} expected={depth} key={key} node={start}")

        if left_state[3] is not None and left_state[3] >= this_depth:
            raise Exception(f"MST left subtree depth >= entry depth: left={left_state[3]} entry={this_depth} key={key} node={start}")

        if right_state[3] is not None and right_state[3] >= this_depth:
            raise Exception(f"MST right subtree depth >= entry depth: right={right_state[3]} entry={this_depth} key={key} node={start}")

        if (
            target_v is not None
            and entry["v"] == target_v
            and (target_k is None or target_k[0] == key)
        ):
            found = True

        last_key = right_state[2] if right_state[2] is not None else key

    if last_key == b"":
        last_key = None

    if first_key is not None and last_key is not None and last_key < first_key:
        raise Exception(f"MST last key less than first key: last_key={last_key} first_key={first_key} node={start}")

    first_key = left_state[1] or first_key

    return found, first_key, last_key, depth, max_levels

def mst_dfs_dump(tree, visited, start, levels=0):
    if start is None:
        return [], None, levels

    if start in visited:
        raise Exception(f"MST cycle detected: node={start}")

    visited.add(start)

    if start not in tree:
        return [], None, levels - 1

    node = tree[start]

    values, left_depth, max_levels = mst_dfs_dump(tree, visited, node["l"], levels + 1)

    key = b""
    depth = None
    first_key = None
    last_key = values[-1][0] if len(values) > 0 else None

    for entry in node["e"]:
        key = key[: entry["p"]] + entry["k"]

        if first_key is None:
            first_key = key

        if last_key is not None and last_key >= key:
            raise Exception(f"MST key not greater than previous: key={key} last_key={last_key} node={start}")

        values.append((key, entry["v"]))

        right_values, right_depth, right_max_levels = mst_dfs_dump(tree, visited, entry["t"], levels + 1)

        values.extend(right_values)

        max_levels = max(max_levels, right_max_levels)

        right_first_key = right_values[0][0] if len(right_values) > 0 else None
        if right_first_key is not None and right_first_key <= key:
            raise Exception(f"MST right subtree first key not greater than parent: right_first={right_first_key} key={key} node={start}")

        this_depth = count_zeroes(hashlib.sha256(key).digest()) // 2
        if depth is None:
            depth = this_depth
        if depth != this_depth:
            raise Exception(f"MST node has entries with different depths: depth={this_depth} expected={depth} key={key} node={start}")

        if (
            (left_depth is not None and left_depth >= this_depth)
            or (right_depth is not None and right_depth >= this_depth)
        ):
            raise Exception(f"MST subtree depth >= entry depth: left={left_depth} right={right_depth} entry={this_depth} key={key} node={start}")

        last_key = right_values[-1][0] if len(right_values) > 0 else key

    if last_key == b"":
        last_key = None

    if first_key is not None and last_key is not None and last_key < first_key:
        raise Exception(f"MST last key less than first key: last_key={last_key} first_key={first_key} node={start}")

    return values, depth, max_levels