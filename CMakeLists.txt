project(mp1)

set(CMAKE_CXX_STANDARD 11)

add_executable(
        mp1
        Application.cpp Application.h
        EmulNet.cpp EmulNet.h
        Entry.h Entry.cpp
        HashTable.h HashTable.cpp
        Log.cpp Log.h
        Member.cpp Member.h
        Message.h Message.cpp
        MP1Node.cpp MP1Node.h
        MP2Node.h MP2Node.cpp
        Node.h Node.cpp
        Params.cpp Params.h
        Queue.h
        stdincludes.h
)