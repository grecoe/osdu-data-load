import os
import typing
import uuid

class FileClass:
    def __init__(self, parent_dir:typing.List[str], data_extension:str):
        """
        Parameters:

        parent_dir: A list of strings identifying folders. If you have a structure
                    to the folder you need to use the full path (not including the 
                    drive identity)

                    /path1/path2

                    If you think you have only one folder in the full share structure
                    then you can just use the name of the leaf directory on it's own

                    path

        data_extension: The lower case file extension, i.e. pdf, without a dot 
        """
        # Identity
        self.identity = str(uuid.uuid4())

        # Paths to parents but make sure to normalize separators
        parent_paths = []
        for p in parent_dir:
            parent_paths.append(FileClass.correct_dir_path(p))
        self.parent_dir:typing.List[str] = parent_paths

        # File extensions to look for. 
        self.data_extension = data_extension
        self.supported_paths = []
        self.loaded_paths = []
        self.files = []

    @staticmethod
    def correct_dir_path(path:str, char_replace = '\\', char_replacement = '/') -> str:
        return path.replace(char_replace, char_replacement)

class Mount:
    @staticmethod
    def load_files(classes:typing.List[FileClass], mount:str):

        normalized_mount = FileClass.correct_dir_path(mount)

        if not os.path.exists(normalized_mount):
            raise Exception("Share mount {} is not valid".format(normalized_mount))

        for root, dirs, files in os.walk(mount, topdown=True):

            # NEW
            normalized_root = FileClass.correct_dir_path(root)
            print("Scanning {}".format(normalized_root))

            full_path = "/{}".format(normalized_root.replace(normalized_mount, ""))
            leaf_path = os.path.split(normalized_root)[-1]

            class_collection = [x for x in classes if full_path in x.parent_dir]
            collection_ids = [x.identity for x in class_collection]
            leaf_collection = [x for x in classes if leaf_path in x.parent_dir]

            for file_class in leaf_collection:
                if file_class.identity not in collection_ids:
                    class_collection.append(file_class)
                    collection_ids.append(file_class.identity)

            if not len(class_collection):
                continue

            # Keep track of every path we run into that matched
            if normalized_root not in class_collection[0].supported_paths:
                class_collection[0].supported_paths.append(normalized_root)

            for file in files:
                extension = file.split('.')[-1]


                if extension.lower() == class_collection[0].data_extension:
                    # Keep track of every path we collect a file from
                    if normalized_root not in class_collection[0].loaded_paths:
                        class_collection[0].loaded_paths.append(normalized_root)

                    # Extension picked up a hit
                    class_collection[0].files.append(os.path.join(normalized_root, file))
            # NEW

            """orig
            root_dir = os.path.split(root)[-1]
            class_collection = [x for x in classes if root_dir in x.paren_dir]

            if not len(class_collection):
                continue
    
            for file in files:
                extension = file.split('.')[-1]

                if extension.lower() == class_collection[0].data_extension:
                    class_collection[0].files.append(os.path.join(root, file))
            """