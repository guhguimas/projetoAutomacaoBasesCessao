

class FileManager:
    def __init__(self):
        self.files = {
            "cessao": None,
            "frontAkrk": None,
            "frontDig": None,
            "credAkrk": None,
            "credDig": None,
            "averbadosAkrk": None,
            "averbadosDig": None,
            "integradosFunc": None,
            "esteirasFunc": None,
        }

    def set_file(self, key, path):
        if key not in self.files:
            raise ValueError(f"Arquivo inválido: {key}")
        
        self.files[key] = path
    
    def get_missing_files(self):
        missing = []
        for key, path in self.files.items():
            if path is None:
                missing.append(key)
        return missing

    def is_complete(self):
        missing = self.get_missing_files()
        return len(missing) == 0

    def reset(self):
        for key in self.files:
            self.files[key] = None

    def snapshot(self):
        return dict(self.files)

    def restore(self, snapshot):
        self.files = dict(snapshot)