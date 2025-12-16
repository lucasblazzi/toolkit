from abc import ABC, abstractmethod


class Stock:
    def __init__(self, symbol: str, price: float):
        self.symbol = symbol
        self.price = price
        self._observers = []

    def attach(self, observer: 'Observer'):
        self._observers.append(observer)

    def detach(self, observer: 'Observer'):
        self._observers.remove(observer)

    def notify(self):
        for observer in self._observers:
            observer.update(self)

    def update_price(self, new_price: float):
        self.price = new_price
        print(f"\n[{self.symbol}] Price updated to: {self.price}")
        self.notify()


class Observer(ABC):
    @abstractmethod
    def update(self, stock: Stock):
        pass


class DashboardDisplay(Observer):
    def update(self, stock: Stock):
        print(f"DashboardDisplay: {stock.symbol} is now {stock.price}")


class ComplianceAlert(Observer):
    def update(self, stock: Stock):
        if stock.price > 150:  # Critério arbitrário para alerta
            print(f"ComplianceAlert: {stock.symbol} price exceeded threshold!")


class AuditLog(Observer):
    def update(self, stock: Stock):
        print(f"AuditLog: Recorded price change for {stock.symbol} to {stock.price}")


if __name__ == "__main__":
    stock = Stock("AAPL", 145.0)
    dashboard = DashboardDisplay()
    compliance = ComplianceAlert()
    audit = AuditLog()
    
    stock.attach(dashboard)
    stock.attach(compliance)
    stock.attach(audit)
    
    stock.update_price(148.0)
    stock.update_price(155.0)
