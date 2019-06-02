package files

import (
    "mime/multipart"
    "io/ioutil"
    "path"
    "os"
    "fmt"
)

func GetSize(f multipart.File) (int, error) {
    content, err := ioutil.ReadAll(f)

    return len(content), err
}

func GetExt(fileName string) string {
    return path.Ext(fileName)
}

func CheckNotExist(src string) bool {
    _, err := os.Stat(src)

    return os.IsNotExist(err)
}

func CheckPermission(src string) bool {
    _, err := os.Stat(src)

    return os.IsPermission(err)
}

func IsNotExistMkDir(src string) error {
    if notExist := CheckNotExist(src); notExist == true {
        if err := MkDir(src); err != nil {
            return err
        }
    }

    return nil
}

func MkDir(src string) error {
    err := os.MkdirAll(src, os.ModePerm)
    if err != nil {
        return err
    }

    return nil
}

func Open(name string, flag int, perm os.FileMode) (*os.File, error) {
    f, err := os.OpenFile(name, flag, perm)
    if err != nil {
        return nil, err
    }

    return f, nil
}

func MustOpen(fileName, dir string) (*os.File, error) {
    perm := CheckPermission(dir)
    if perm == true {
        return nil, fmt.Errorf("permission denied dir: %s", dir)
    }

    err := IsNotExistMkDir(dir)
    if err != nil {
        return nil, fmt.Errorf("error during make dir %s, err: %s", dir, err)
    }

    f, err := Open(dir + string(os.PathSeparator) + fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
    if err != nil {
        return nil, fmt.Errorf("fail to open file, err: %s", err)
    }

    return f, nil
}
