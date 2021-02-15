import pandas as pd

class Collate:

    def run(self, *args):
        return (
            pd.concat([e[i] for e in args]).sort_index()
            for i in range(len(args[0]))
        )
