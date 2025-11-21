from dataclasses import dataclass

@dataclass
class ClaimData:
    content: str
    source: str
    date_added: str
    claim_date: str