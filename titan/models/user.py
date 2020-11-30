from passlib.context import CryptContext
from pydantic import BaseModel, validator

# security


class Token(BaseModel):
    access_token: str
    token_type: str


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# users


class UserBase(BaseModel):
    username: str
    fullname: str = None
    email: str


class UserCreateRequest(UserBase):
    password: str

    @validator("username")
    def username_alphanumeric(cls, v):
        assert v.isalnum(), "must be alphanumeric"
        return v

    @validator("email")
    def email_is_valid(cls, v):
        assert "@" in v, "email is not valid"
        return v

    @property
    def hashed_password(self) -> str:
        return pwd_context.hash(self.password)


class User(UserBase):
    disabled: bool = False


class UserInDB(User):
    password: str

    def verify_password(self, plain_password) -> bool:
        return pwd_context.verify(plain_password, self.password)
