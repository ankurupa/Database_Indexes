Database_Indexes
================

The project implements the Hash Index and ISAM Index over the database tuples. It also validates the index generated and reads the tuples with the help of database indexes.

The Buffer Manager is a simplifiewd version of a buffer manager that might be found in a traditional database. It keeps the sequence of frames, and allocates them on as needed basis to files.

The primary interface to the Buffer Manager is the FileManager and ManagaedFile classes. Each ManagedFile corresponds to a single file on disk, and gives per-page access to the contents of the file.
ManagedFile interacts with the Buffer Manager to limit the memory usage to an amoubnt specified when the Buffer Manager is first created. File Manager provides a means of creating new managedFile instances.

