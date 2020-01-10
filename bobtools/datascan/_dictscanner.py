import logging
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable

leaf = {"types": [], "prototype": None, "values": [], "count": 0}


@dataclass
class DictScanner:
    iterable: Iterable = field(default_factory=lambda: [])
    n_scanned: int = 0
    root: dict = field(default_factory=lambda: {})

    def scan_all(self, iterable: Iterable) -> None:
        for i in iterable:
            self.scan_this(i)

    def scan_one(self, iterable: Iterable) -> None:
        for i in iterable:
            self.scan_this(i)
            return
        pass

    def scan_this(self, example: Dict) -> None:
        self.n_scanned += 1
        flat_example = self._flatten(example)
        self._update_root(flat_example)

    def schema(self):
        pass

    @staticmethod
    def _mergedict(nested_dict: dict) -> dict:
        newdict = {}
        for k, v in nested_dict.items():
            if k == "__list__":
                if type(nested_dict[k]) == dict:
                    return [DictScanner._mergedict(nested_dict[k])]
                else:
                    return [nested_dict[k]]
            if type(v) == dict:
                newdict[k] = DictScanner._mergedict(v)
            else:
                newdict[k] = v
        return newdict

    @staticmethod
    def _reconstruct_dict(root_representation: dict, extractor: Callable) -> dict:
        reconstructed_dict = {}
        for k, v in root_representation.items():
            place = reconstructed_dict
            last = len(k) - 1
            for n, key in enumerate(k):
                if n == last:
                    # skip if key is known and update value is based on None leaf
                    if key in place and root_representation[k]["prototype"] is None:
                        continue
                    place[key] = extractor(v)
                    break
                else:
                    if key not in place:
                        place[key] = {}
                        place = place[key]
                    elif type(place[key]) != dict:
                        # if the point before ended in Nonde
                        if root_representation[k[: n + 1]]["prototype"] is None:
                            place[key] = {}
                        # if this point was a leaf before, but is not now
                        else:
                            logging.warning(
                                f"key at {k} is sometimes nested, sometimes value!"
                            )
                            place = {}
                    else:
                        place = place[key]
        return DictScanner._mergedict(reconstructed_dict)

    def to_schema(self) -> dict:
        schema_from_leaf = (
            lambda v: v["types"][0]
            if len(set(v["types"]).difference(set([type(None)]))) <= 1
            else "multiple"
        )
        return self._reconstruct_dict(self.root, extractor=schema_from_leaf)

    def to_prototype(self) -> dict:
        prototype_from_leaf = lambda v: v["prototype"]  # noqa
        return self._reconstruct_dict(self.root, prototype_from_leaf)

    @staticmethod
    def _flatten(datamap, trail: tuple = ()) -> dict:
        flatmap = {}
        if type(datamap) != dict:
            return {trail: datamap}
        for k, v in datamap.items():
            loc = trail + (k,)
            if type(v) == dict:
                flatmap.update(DictScanner._flatten(v, loc))
            elif type(v) == list:
                loc = trail + (k, "__list__")
                for child in v:
                    flatmap.update(DictScanner._flatten(child, loc))
            else:
                flatmap[loc] = v
        return flatmap

    def _update_root(self, flatdict: Dict):
        for trail, v in flatdict.items():
            if trail not in self.root:
                self.root[trail] = deepcopy(leaf)
            self.root[trail]["types"].append(type(v))
            self.root[trail]["count"] += 1
            self.root[trail]["values"].append(v)
            if not isinstance(v, type(None)):
                self.root[trail]["prototype"] = v
