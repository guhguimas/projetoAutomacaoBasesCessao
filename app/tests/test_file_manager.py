from app.core.file_manager import FileManager

fm = FileManager()
# fm.set_file("cessao", "x.xlsx")
# fm.set_file("frontAkrk", "y.xlsx")
print(fm.get_missing_files())
print(fm.is_complete())