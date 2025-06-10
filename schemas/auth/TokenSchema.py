from pydantic import BaseModel


class TokenSchema(BaseModel):
    access_token: str
    token_type: str
    cookie_name: str
    expires: int
    httpOnly: bool
